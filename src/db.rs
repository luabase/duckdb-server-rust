use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use duckdb::{params_from_iter, types::ToSql, AccessMode, Config, DuckdbConnectionManager};
use std::collections::HashSet;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::time::Duration;
use tracing::log::info;

use crate::constants::AUTOINSTALL_QUERY;
use crate::interfaces::{AppError, Extension, SqlValue};
use crate::sql::{enforce_query_limit, is_writable_sql};


#[async_trait]
pub trait Database: Send + Sync {
    async fn execute(&self, sql: &str, extensions: Option<&[Extension]>) -> Result<()>;
    async fn get_json(&self, sql: String, args: &[SqlValue], prepare_sql: Option<String>, limit: usize, extensions: Option<&[Extension]>) -> Result<Vec<u8>>;
    async fn get_arrow(&self, sql: String, args: &[SqlValue], prepare_sql: Option<String>, limit: usize, extensions: Option<&[Extension]>) -> Result<Vec<u8>>;
    async fn get_record_batches(&self, sql: String, args: &[SqlValue], prepare_sql: Option<String>, limit: usize, extensions: Option<&[Extension]>) -> Result<Vec<RecordBatch>>;
    fn reconnect(&self) -> Result<()>;
    fn status(&self) -> Result<PoolStatus, AppError>;
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

pub struct ConnectionPool {
    db_path: String,
    pool_size: u32,
    timeout: Duration,
    access_mode: AccessMode,
    pool: parking_lot::RwLock<r2d2::Pool<DuckdbConnectionManager>>,
    inode: parking_lot::RwLock<u64>,
    loaded_extensions: parking_lot::RwLock<HashSet<String>>
}

impl ConnectionPool {
    pub fn new(db_path: &str, pool_size: u32, timeout: Duration, access_mode: AccessMode) -> Result<Self> {
        info!(
            "Creating connection pool: db_path={}, pool_size={}, access_mode={:?}, timeout={:?}",
            db_path, pool_size, access_mode, timeout
        );

        let inode = std::fs::metadata(db_path)?.ino();
        let pool = Self::create_pool(db_path, pool_size, timeout, &access_mode)?;

        Ok(Self {
            db_path: db_path.to_string(),
            pool_size,
            timeout,
            access_mode,
            pool: parking_lot::RwLock::new(pool),
            inode: parking_lot::RwLock::new(inode),
            loaded_extensions: parking_lot::RwLock::new(HashSet::new())
        })
    }

    pub fn get(&self) -> Result<r2d2::PooledConnection<DuckdbConnectionManager>, AppError> {
        info!("Checking out connection from pool: db_path={}", self.db_path);

        let current_inode = match std::fs::metadata(&self.db_path) {
            Ok(meta) => meta.ino(),
            Err(_) => {
                info!("DuckDB file missing or inaccessible; attempting to rebuild pool");
                self.reset_pool(None).map_err(|e| AppError::Error(e))?;
                let pool_guard = self.pool.read();
                return pool_guard.get().map_err(|e| {
                    let err_str = e.to_string().to_lowercase();
                    if err_str.contains("timeout") {
                        AppError::Timeout
                    } else {
                        AppError::Error(anyhow::anyhow!("Pool error: {}", e))
                    }
                });
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
                let (new_pool, _) = self.reset_pool_internal().map_err(|e| AppError::Error(e))?;
                *self.pool.write() = new_pool;
                *write_guard = current_inode;
            }
        }

        let pool_guard = self.pool.read();
        pool_guard.get().map_err(|e| {
            let err_str = e.to_string().to_lowercase();
            if err_str.contains("timeout") {
                AppError::Timeout
            } else {
                AppError::Error(anyhow::anyhow!("Pool error: {}", e))
            }
        })
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
        let new_pool = Self::create_pool(&self.db_path, self.pool_size, self.timeout, &self.access_mode)?;
        let inode = std::fs::metadata(&self.db_path)?.ino();

        Ok((new_pool, inode))
    }

    fn create_pool(
        db_path: &str, 
        pool_size: u32, 
        timeout: Duration, 
        access_mode: &AccessMode
    ) -> Result<r2d2::Pool<DuckdbConnectionManager>> {
        let config = Config::default()
            .access_mode(match access_mode {
                AccessMode::ReadOnly => AccessMode::ReadOnly,
                AccessMode::ReadWrite => AccessMode::ReadWrite,
                AccessMode::Automatic => AccessMode::Automatic,
            })?
            .allow_unsigned_extensions()?
            .enable_autoload_extension(true)?
            .threads(pool_size as i64)?;

        let manager = DuckdbConnectionManager::file_with_flags(db_path, config)?;
        let pool = r2d2::Pool::builder()
            .max_size(pool_size)
            .min_idle(Some(1))
            .connection_timeout(timeout)
            .build(manager)?;

        _ = pool.get()?.execute_batch(AUTOINSTALL_QUERY);

        Ok(pool)
    }
}

#[async_trait]
impl Database for Arc<ConnectionPool> {
    async fn execute(&self, sql: &str, extensions: Option<&[Extension]>) -> Result<()> {
        let conn = self.get().map_err(|e| anyhow::anyhow!("{}", e))?;
        
        if let Some(exts) = extensions {
            self.load_extensions(&conn, exts)?;
        }
        
        conn.execute_batch(sql)?;

        if is_writable_sql(sql) {
            self.reset_pool(None)?;
        }

        Ok(())
    }

    async fn get_json(&self, 
        sql: String, 
        args: &[SqlValue], 
        prepare_sql: Option<String>, 
        limit: usize, 
        extensions: Option<&[Extension]>
    ) -> Result<Vec<u8>> {
        let sql_owned = sql.clone();
        let prepare_sql_owned = prepare_sql.clone();
        let effective_sql = enforce_query_limit(&sql_owned, limit)?;
        let args = args.to_vec();
        let pool = Arc::clone(self);
        let extensions_owned = extensions.map(|exts| exts.to_vec());

        let result = tokio::task::spawn_blocking({
            move || -> Result<Vec<u8>> {
                let conn = pool.get().map_err(|e| anyhow::anyhow!("{}", e))?;
                
                if let Some(exts) = &extensions_owned {
                    pool.load_extensions(&conn, exts)?;
                }

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

    async fn get_arrow(&self, 
        sql: String, 
        args: &[SqlValue], 
        prepare_sql: Option<String>, 
        limit: usize, 
        extensions: Option<&[Extension]>
    ) -> Result<Vec<u8>> {
        let sql_owned = sql.clone();
        let prepare_sql_owned = prepare_sql.clone();
        let effective_sql = enforce_query_limit(&sql_owned, limit)?;
        let args = args.to_vec();
        let pool = Arc::clone(self);
        let extensions_owned = extensions.map(|exts| exts.to_vec());

        let result = tokio::task::spawn_blocking({
            move || -> Result<Vec<u8>> {
                let conn = pool.get().map_err(|e| anyhow::anyhow!("{}", e))?;
                
                if let Some(exts) = &extensions_owned {
                    pool.load_extensions(&conn, exts)?;
                }

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

    async fn get_record_batches(
        &self, 
        sql: String, 
        args: &[SqlValue], 
        prepare_sql: Option<String>, 
        limit: usize, 
        extensions: Option<&[Extension]>
    ) -> Result<Vec<RecordBatch>> {
        let sql_owned = sql.clone();
        let effective_sql = enforce_query_limit(&sql_owned, limit)?;
        let args = args.to_vec();
        let pool = Arc::clone(self);
        let prepare_sql_owned = prepare_sql.clone();
        let extensions_owned = extensions.map(|exts| exts.to_vec());

        let result = tokio::task::spawn_blocking({
            move || -> Result<Vec<RecordBatch>> {
                let conn = pool.get().map_err(|e| anyhow::anyhow!("{}", e))?;
                
                if let Some(exts) = &extensions_owned {
                    pool.load_extensions(&conn, exts)?;
                }

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

    fn status(&self) -> Result<PoolStatus, AppError> {
        let pool_guard = self.pool.read();
        let pool_info = pool_guard.state();

        Ok(PoolStatus {
            db_path: self.db_path.clone(),
            pool_size: self.pool_size as usize,
            access_mode: format!("{:?}", self.access_mode),
            in_use: (pool_info.connections - pool_info.idle_connections) as usize,
            idle: pool_info.idle_connections as usize,
            total: pool_info.connections as usize,
            timeout: self.timeout,
        })
    }
}

impl ConnectionPool {
    fn load_extensions(&self, conn: &duckdb::Connection, extensions: &[Extension]) -> Result<()> {
        let mut loaded_set = self.loaded_extensions.write();
        
        let extensions_to_check: Vec<_> = extensions.iter()
            .filter(|ext| !loaded_set.contains(&ext.name))
            .collect();
        
        if extensions_to_check.is_empty() {
            info!("All requested extensions already loaded");
            return Ok(());
        }
        
        info!(
            "Processing {} extensions: {:?}", 
            extensions_to_check.len(), 
            extensions_to_check.iter().map(|e| &e.name).collect::<Vec<_>>()
        );
        
        let extension_names: Vec<_> = extensions_to_check.iter()
            .map(|ext| ext.name.as_str())
            .collect();
        
        let placeholders = std::iter::repeat("?")
            .take(extension_names.len())
            .collect::<Vec<_>>()
            .join(",");

        let query = format!(
            "SELECT extension_name, installed, loaded FROM duckdb_extensions() WHERE extension_name IN ({})", 
            placeholders
        );
        
        let mut stmt = conn.prepare(&query)?;
        let result = stmt.query_arrow(params_from_iter(extension_names.iter()))?;
        let batches: Vec<_> = result.collect();
        
        let mut current_states = std::collections::HashMap::new();
        for batch in batches {
            if batch.num_columns() >= 3 {
                let name_col = batch.column(0);
                let installed_col = batch.column(1);
                let loaded_col = batch.column(2);
                
                if let (Some(name_array), Some(installed_array), Some(loaded_array)) = (
                    name_col.as_any().downcast_ref::<arrow::array::StringArray>(),
                    installed_col.as_any().downcast_ref::<arrow::array::BooleanArray>(),
                    loaded_col.as_any().downcast_ref::<arrow::array::BooleanArray>()
                ) {
                    for i in 0..batch.num_rows() {
                        let name = name_array.value(i).to_string();
                        let installed = installed_array.value(i);
                        let loaded = loaded_array.value(i);
                        current_states.insert(name, (installed, loaded));
                    }
                }
            }
        }
        
        let mut install_commands = Vec::new();
        for ext in &extensions_to_check {
            let (installed, loaded) = current_states.get(&ext.name).unwrap_or(&(false, false));
            
            if *loaded {
                info!("Extension '{}' already loaded", ext.name);
                loaded_set.insert(ext.name.clone());
            } else if !*installed {
                let install_sql = if let Some(source) = &ext.source {
                    info!("Installing extension {} from source {}", ext.name, source);
                    format!("INSTALL {} FROM {}", ext.name, source)
                } else {
                    info!("Installing extension '{}'", ext.name);
                    format!("INSTALL {}", ext.name)
                };
                install_commands.push(install_sql);
            }
        }
        
        if !install_commands.is_empty() {
            let install_batch = install_commands.join(";\n");
            conn.execute_batch(&install_batch)?;
        }
        
        let mut load_commands = Vec::new();
        for ext in &extensions_to_check {
            let (_installed, loaded) = current_states.get(&ext.name).unwrap_or(&(false, false));
            
            if !*loaded {
                info!("Loading extension {}", ext.name);
                load_commands.push(format!("LOAD {}", ext.name));
            }
        }
        
        if !load_commands.is_empty() {
            let load_batch = load_commands.join(";\n");
            conn.execute_batch(&load_batch)?;
        }
        
        for ext in extensions_to_check {
            loaded_set.insert(ext.name.clone());
        }

        Ok(())
    }
}
