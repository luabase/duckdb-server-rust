use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use duckdb::{AccessMode, Config, DuckdbConnectionManager, params_from_iter, types::ToSql};

use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::log::info;

use crate::constants::AUTOINSTALL_QUERY;
use crate::interfaces::{AppError, DucklakeConfig, Extension, SecretConfig, SqlValue};
use crate::sql::{enforce_query_limit, is_writable_sql};

#[async_trait]
pub trait Database: Send + Sync {
    async fn execute(&self, sql: &str, extensions: Option<&[Extension]>) -> Result<()>;
    async fn get_json(
        &self,
        sql: &String,
        args: &Option<Vec<SqlValue>>,
        prepare_sql: &Option<String>,
        limit: usize,
        extensions: Option<&[Extension]>,
        cancel_token: CancellationToken,
    ) -> Result<Vec<u8>>;
    async fn get_arrow(
        &self,
        sql: &String,
        args: &Option<Vec<SqlValue>>,
        prepare_sql: &Option<String>,
        limit: usize,
        extensions: Option<&[Extension]>,
        cancel_token: CancellationToken,
    ) -> Result<Vec<u8>>;
    async fn get_record_batches(
        &self,
        sql: &String,
        args: &Option<Vec<SqlValue>>,
        prepare_sql: &Option<String>,
        limit: usize,
        extensions: Option<&[Extension]>,
        cancel_token: CancellationToken,
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

pub struct ConnectionPool {
    db_path: String,
    pool_size: u32,
    timeout: Duration,
    access_mode: AccessMode,
    pool: parking_lot::RwLock<r2d2::Pool<DuckdbConnectionManager>>,
    inode: parking_lot::RwLock<u64>,
    secrets: Option<Vec<SecretConfig>>,
    ducklakes: Option<Vec<DucklakeConfig>>,
}

impl ConnectionPool {
    pub fn new(
        db_path: &str, 
        pool_size: u32, 
        timeout: Duration, 
        access_mode: AccessMode, 
        secrets: &Option<Vec<SecretConfig>>,
        ducklakes: &Option<Vec<DucklakeConfig>>,
    ) -> Result<Self> {
        info!(
            "Creating connection pool: db_path={}, pool_size={}, access_mode={:?}, timeout={:?}",
            db_path, pool_size, access_mode, timeout
        );

        let inode = std::fs::metadata(db_path)?.ino();
        let pool = Self::create_pool(
            db_path, 
            pool_size, 
            timeout, 
            &access_mode, 
            &ducklakes, 
            &secrets
        )?;

        Ok(Self {
            db_path: db_path.to_string(),
            pool_size,
            timeout,
            access_mode,
            pool: parking_lot::RwLock::new(pool),
            inode: parking_lot::RwLock::new(inode),
            secrets: secrets.clone(),
            ducklakes: ducklakes.clone(),
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
                    }
                    else {
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
            }
            else {
                AppError::Error(anyhow::anyhow!("Pool error: {}", e))
            }
        })
    }

    pub fn reset_pool(&self, new_inode: Option<u64>) -> Result<()> {
        let (new_pool, detected_inode) = self.reset_pool_internal()?;
        *self.pool.write() = new_pool;

        if let Some(inode_val) = new_inode {
            *self.inode.write() = inode_val;
        }
        else {
            *self.inode.write() = detected_inode;
        }

        Ok(())
    }

    fn reset_pool_internal(&self) -> Result<(r2d2::Pool<DuckdbConnectionManager>, u64)> {
        let new_pool = Self::create_pool(
            &self.db_path, 
            self.pool_size, 
            self.timeout, 
            &self.access_mode, 
            &self.ducklakes, 
            &self.secrets
        )?;

        let inode = std::fs::metadata(&self.db_path)?.ino();

        Ok((new_pool, inode))
    }

    fn create_pool(
        db_path: &str,
        pool_size: u32,
        timeout: Duration,
        access_mode: &AccessMode,
        ducklakes: &Option<Vec<DucklakeConfig>>,
        secrets: &Option<Vec<SecretConfig>>,
    ) -> Result<r2d2::Pool<DuckdbConnectionManager>> {
        let config = Config::default()
            .access_mode(match access_mode {
                AccessMode::ReadOnly => AccessMode::ReadOnly,
                AccessMode::ReadWrite => AccessMode::ReadWrite,
                AccessMode::Automatic => AccessMode::Automatic,
            })?
            .allow_unsigned_extensions()?
            .enable_autoload_extension(true)?
            .enable_object_cache(true)?
            .threads(pool_size as i64)?;

        let manager = DuckdbConnectionManager::file_with_flags(db_path, config)?;
        let pool = r2d2::Pool::builder()
            .max_size(pool_size)
            .min_idle(Some(1))
            .connection_timeout(timeout)
            .build(manager)?;

        let conn = pool.get()?;

        _ = conn.execute_batch(&(AUTOINSTALL_QUERY.join(";\n") + ";"))?;

        if let Some(secrets) = secrets {
            for secret in secrets {
                let (sql, args) = Self::build_create_secret_query(secret);
                conn.execute(&sql, params_from_iter(args.iter()))?;
            }
        }

        if let Some(ducklakes) = ducklakes {
            for ducklake in ducklakes {
                let (sql, args) = Self::build_attach_ducklake_query(ducklake);
                conn.execute(&sql, params_from_iter(args.iter()))?;
            }
        }

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

    async fn get_json(
        &self,
        sql: &String,
        args: &Option<Vec<SqlValue>>,
        prepare_sql: &Option<String>,
        limit: usize,
        extensions: Option<&[Extension]>,
        cancel_token: CancellationToken,
    ) -> Result<Vec<u8>> {
        let sql_owned = sql.clone();
        let prepare_sql_owned = prepare_sql.clone();
        let effective_sql = enforce_query_limit(&sql_owned, limit)?;
        let args = args.clone().unwrap_or_default();
        let pool = Arc::clone(self);
        let extensions_owned = extensions.map(|exts| exts.to_vec());

        let result = tokio::select! {
            result = tokio::task::spawn_blocking({
                let cancel_token = cancel_token.clone();
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
                        if cancel_token.is_cancelled() {
                            return Err(anyhow::anyhow!("Query cancelled"));
                        }
                        writer.write(&batch)?;
                    }
                    writer.finish()?;
                    Ok(writer.into_inner())
                }
            }) => result.map_err(|e| anyhow::anyhow!("Task error: {}", e))?,
            _ = cancel_token.cancelled() => {
                return Err(anyhow::anyhow!("Query cancelled"));
            }
        };

        if is_writable_sql(&sql_owned) {
            self.reset_pool(None)?;
        }

        result
    }

    async fn get_arrow(
        &self,
        sql: &String,
        args: &Option<Vec<SqlValue>>,
        prepare_sql: &Option<String>,
        limit: usize,
        extensions: Option<&[Extension]>,
        cancel_token: CancellationToken,
    ) -> Result<Vec<u8>> {
        let sql_owned = sql.clone();
        let prepare_sql_owned = prepare_sql.clone();
        let effective_sql = enforce_query_limit(&sql_owned, limit)?;
        let args = args.clone().unwrap_or_default();
        let pool = Arc::clone(self);
        let extensions_owned = extensions.map(|exts| exts.to_vec());

        let result = tokio::select! {
            result = tokio::task::spawn_blocking({
                let cancel_token = cancel_token.clone();
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
                        if cancel_token.is_cancelled() {
                            return Err(anyhow::anyhow!("Query cancelled"));
                        }
                        writer.write(&batch)?;
                    }
                    writer.finish()?;
                    Ok(buffer)
                }
            }) => result.map_err(|e| anyhow::anyhow!("Task error: {}", e))?,
            _ = cancel_token.cancelled() => {
                return Err(anyhow::anyhow!("Query cancelled"));
            }
        };

        if is_writable_sql(&sql_owned) {
            self.reset_pool(None)?;
        }

        result
    }

    async fn get_record_batches(
        &self,
        sql: &String,
        args: &Option<Vec<SqlValue>>,
        prepare_sql: &Option<String>,
        limit: usize,
        extensions: Option<&[Extension]>,
        cancel_token: CancellationToken,
    ) -> Result<Vec<RecordBatch>> {
        let sql_owned = sql.clone();
        let effective_sql = enforce_query_limit(&sql_owned, limit)?;
        let args = args.clone().unwrap_or_default();
        let pool = Arc::clone(self);
        let prepare_sql_owned = prepare_sql.clone();
        let extensions_owned = extensions.map(|exts| exts.to_vec());

        let result = tokio::select! {
            result = tokio::task::spawn_blocking({
                let cancel_token = cancel_token.clone();
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

                    let mut batches = Vec::new();
                    for batch in arrow {
                        if cancel_token.is_cancelled() {
                            return Err(anyhow::anyhow!("Query cancelled"));
                        }
                        batches.push(batch);
                    }
                    Ok(batches)
                }
            }) => result.map_err(|e| anyhow::anyhow!("Task error: {}", e))?,
            _ = cancel_token.cancelled() => {
                return Err(anyhow::anyhow!("Query cancelled"));
            }
        };

        if is_writable_sql(&sql_owned) {
            self.reset_pool(None)?;
        }

        result
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

    fn kill_all_connections(&self) -> Result<()> {
        info!("Interrupting all connections in pool: {}", self.db_path);

        self.reset_pool(None)?;

        Ok(())
    }
}

impl ConnectionPool {
    fn build_create_secret_query(secret_config: &SecretConfig) -> (String, Vec<Box<dyn ToSql>>) {
        let mut query = String::from("CREATE OR REPLACE SECRET ? (TYPE ?, KEY_ID ?");
        let mut params: Vec<Box<dyn ToSql>> = Vec::new();
        params.push(Box::new(secret_config.name.clone()));
        params.push(Box::new(secret_config.secret_type.clone()));
        params.push(Box::new(secret_config.key_id.clone()));

        if let Some(secret) = &secret_config.secret {
            query.push_str(", SECRET ?");
            params.push(Box::new(secret.clone()));
        }
        if let Some(provider) = &secret_config.provider {
            query.push_str(", PROVIDER ?");
            params.push(Box::new(provider.clone()));
        }
        if let Some(region) = &secret_config.region {
            query.push_str(", REGION ?");
            params.push(Box::new(region.clone()));
        }
        if let Some(token) = &secret_config.token {
            query.push_str(", TOKEN ?");
            params.push(Box::new(token.clone()));
        }
        if let Some(scope) = &secret_config.scope {
            query.push_str(", SCOPE ?");
            params.push(Box::new(scope.clone()));
        }
        query.push(')');
        (query, params)
    }

    fn build_attach_ducklake_query(ducklake_config: &DucklakeConfig) -> (String, Vec<Box<dyn ToSql>>) {
        let mut query = String::from("ATTACH ? AS ? (DATA_PATH ?");
        let mut params: Vec<Box<dyn ToSql>> = Vec::new();
        params.push(Box::new(ducklake_config.connection.clone()));
        params.push(Box::new(ducklake_config.alias.clone()));
        params.push(Box::new(ducklake_config.data_path.clone()));
        if let Some(meta_schema) = &ducklake_config.meta_schema {
            query.push_str(", META_SCHEMA ?");
            params.push(Box::new(meta_schema.clone()));
        }
        query.push(')');
        (query, params)
    }

    fn load_extensions(&self, conn: &duckdb::Connection, extensions: &[Extension]) -> Result<()> {
        if extensions.is_empty() {
            info!("No extensions to load");
            return Ok(());
        }

        info!(
            "Loading {} extensions: {:?}",
            extensions.len(),
            extensions.iter().map(|e| &e.name).collect::<Vec<_>>()
        );

        let mut commands = Vec::new();
        for ext in extensions {
            let install_sql = if let Some(source) = &ext.source {
                info!("Installing extension {} from source {}", ext.name, source);
                format!("INSTALL {} FROM {}", ext.name, source)
            }
            else {
                info!("Installing extension {}", ext.name);
                format!("INSTALL {}", ext.name)
            };
            commands.push(install_sql);

            info!("Loading extension {}", ext.name);
            commands.push(format!("LOAD {}", ext.name));
        }

        if !commands.is_empty() {
            let batch = commands.join(";\n");
            conn.execute_batch(&batch)?;
        }

        Ok(())
    }
}
