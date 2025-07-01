use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use duckdb::{params_from_iter, types::ToSql, AccessMode, Config, DuckdbConnectionManager};
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use tracing::log::info;

use crate::constants::AUTOINSTALL_QUERY;
use crate::interfaces::SqlValue;
use crate::sql::{enforce_query_limit, is_writable_sql};

#[async_trait]
pub trait Database: Send + Sync {
    async fn execute(&self, sql: &str) -> Result<()>;
    async fn get_json(&self, sql: String, args: &[SqlValue], prepare_sql: Option<String>, limit: usize) -> Result<Vec<u8>>;
    async fn get_arrow(&self, sql: String, args: &[SqlValue], prepare_sql: Option<String>, limit: usize) -> Result<Vec<u8>>;
    async fn get_record_batches(&self, sql: String, args: &[SqlValue], prepare_sql: Option<String>, limit: usize) -> Result<Vec<RecordBatch>>;
    fn reconnect(&self) -> Result<()>;
}

pub struct ConnectionPool {
    db_path: String,
    pool_size: u32,
    access_mode: AccessMode,
    pool: parking_lot::RwLock<r2d2::Pool<DuckdbConnectionManager>>,
    inode: parking_lot::RwLock<u64>
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
            .allow_unsigned_extensions()?
            .enable_autoload_extension(true)?
            .threads(pool_size as i64)?;

        let inode = std::fs::metadata(db_path)?.ino();
        let manager = DuckdbConnectionManager::file_with_flags(db_path, config)?;
        let pool = r2d2::Pool::builder().max_size(pool_size).build(manager)?;

        _ = pool.get()?.execute_batch(AUTOINSTALL_QUERY);

        Ok(Self {
            db_path: db_path.to_string(),
            pool_size,
            access_mode,
            pool: parking_lot::RwLock::new(pool),
            inode: parking_lot::RwLock::new(inode)
        })
    }

    pub fn get(&self) -> Result<r2d2::PooledConnection<DuckdbConnectionManager>> {
        info!("Checking out connection from pool: db_path={}", self.db_path);

        let current_inode = match std::fs::metadata(&self.db_path) {
            Ok(meta) => meta.ino(),
            Err(_) => {
                info!("DuckDB file missing or inaccessible; attempting to rebuild pool");
                self.reset_pool(None)?;
                let pool_guard = self.pool.read();
                return Ok(pool_guard.get()?)
            }
        };

        {
            let upg = self.inode.upgradable_read();
            if *upg != current_inode {
                info!(
                    "Detected file change (inode = {}). Rebuilding pool for {}",
                    current_inode, self.db_path
                );

                let mut write_guard = parking_lot::RwLockUpgradableReadGuard::upgrade(upg);
                let (new_pool, _) = self.reset_pool_internal()?;
                *self.pool.write() = new_pool;
                *write_guard = current_inode;
            }
        }

        let pool_guard = self.pool.read();
        Ok(pool_guard.get()?)
    }

    pub fn reset_pool(&self, new_inode: Option<u64>) -> Result<()> {
        let (new_pool, detected_inode) = self.reset_pool_internal()?;
        *self.pool.write() = new_pool;

        if let Some(inode_val) = new_inode {
            *self.inode.write() = inode_val;
        } else {
            *self.inode.write() = detected_inode;
        }

        Ok(())
    }

    fn reset_pool_internal(&self) -> Result<(r2d2::Pool<DuckdbConnectionManager>, u64)> {
        let config = Config::default()
            .access_mode(match self.access_mode {
                AccessMode::ReadOnly => AccessMode::ReadOnly,
                AccessMode::ReadWrite => AccessMode::ReadWrite,
                AccessMode::Automatic => AccessMode::Automatic,
            })?
            .allow_unsigned_extensions()?
            .enable_autoload_extension(true)?
            .threads(self.pool_size as i64)?;

        let manager = DuckdbConnectionManager::file_with_flags(&self.db_path, config)?;
        let new_pool = r2d2::Pool::builder().max_size(self.pool_size).build(manager)?;

        new_pool.get()?.execute_batch(AUTOINSTALL_QUERY)?;
        let inode = std::fs::metadata(&self.db_path)?.ino();

        Ok((new_pool, inode))
    }
}

#[async_trait]
impl Database for Arc<ConnectionPool> {
    async fn execute(&self, sql: &str) -> Result<()> {
        let conn = self.get()?;
        conn.execute_batch(sql)?;

        if is_writable_sql(sql) {
            self.reset_pool(None)?;
        }

        Ok(())
    }

    async fn get_json(&self, sql: String, args: &[SqlValue], prepare_sql: Option<String>, limit: usize) -> Result<Vec<u8>> {
        let sql_owned = sql.clone();
        let prepare_sql_owned = prepare_sql.clone();
        let effective_sql = enforce_query_limit(&sql_owned, limit)?;
        let args = args.to_vec();
        let pool = Arc::clone(self);

        let result = tokio::task::spawn_blocking({
            move || -> Result<Vec<u8>> {
                let conn = pool.get()?;
                if let Some(prepare_sql) = prepare_sql_owned {
                    conn.execute_batch(&prepare_sql)?;
                }

                let mut stmt = conn.prepare(&effective_sql)?;
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
            self.reset_pool(None)?;
        }

        Ok(result)
    }

    async fn get_arrow(&self, sql: String, args: &[SqlValue], prepare_sql: Option<String>, limit: usize) -> Result<Vec<u8>> {
        let sql_owned = sql.clone();
        let prepare_sql_owned = prepare_sql.clone();
        let effective_sql = enforce_query_limit(&sql_owned, limit)?;
        let args = args.to_vec();
        let pool = Arc::clone(self);

        let result = tokio::task::spawn_blocking({
            move || -> Result<Vec<u8>> {
                let conn = pool.get()?;
                if let Some(prepare_sql) = prepare_sql_owned {
                    conn.execute_batch(&prepare_sql)?;
                }

                let mut stmt = conn.prepare(&effective_sql)?;
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
            self.reset_pool(None)?;
        }

        Ok(result)
    }

    async fn get_record_batches(&self, sql: String, args: &[SqlValue], prepare_sql: Option<String>, limit: usize) -> Result<Vec<RecordBatch>> {
        let sql_owned = sql.clone();
        let effective_sql = enforce_query_limit(&sql_owned, limit)?;
        let args = args.to_vec();
        let pool = Arc::clone(self);
        let prepare_sql_owned = prepare_sql.clone();

        let result = tokio::task::spawn_blocking({
            move || -> Result<Vec<RecordBatch>> {
                let conn = pool.get()?;
                if let Some(prepare_sql) = prepare_sql_owned {
                    conn.execute_batch(&prepare_sql)?;
                }

                let mut stmt = conn.prepare(&effective_sql)?;
                let tosql_args: Vec<Box<dyn ToSql>> = args.iter().map(|arg| arg.as_tosql()).collect();
                let arrow = stmt.query_arrow(params_from_iter(tosql_args.iter()))?;
                Ok(arrow.collect())
            }
        })
        .await??;

        if is_writable_sql(&sql_owned) {
            self.reset_pool(None)?;
        }

        Ok(result)
    }

    fn reconnect(&self) -> Result<()> {
        self.reset_pool(None)
    }

}
