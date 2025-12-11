use anyhow::Result;
use duckdb::{AccessMode, Config, DuckdbConnectionManager};
use std::os::unix::fs::MetadataExt;
use std::time::Duration;
use tracing::log::info;

use crate::constants::AUTOINSTALL_QUERY;
use crate::interfaces::{AppError, DbType, DucklakeConfig, Extension, SecretConfig};

use super::config::{load_extensions, setup_ducklakes, setup_secrets};
use super::traits::PoolStatus;

pub struct ConnectionPool {
    pub(crate) db: DbType,
    pub(crate) pool_size: u32,
    pub(crate) timeout: Duration,
    pub(crate) access_mode: AccessMode,
    pub(crate) pool: parking_lot::RwLock<r2d2::Pool<DuckdbConnectionManager>>,
    pub(crate) inode: parking_lot::RwLock<Option<u64>>,
    pub(crate) extensions: parking_lot::RwLock<Option<Vec<Extension>>>,
    pub(crate) secrets: parking_lot::RwLock<Option<Vec<SecretConfig>>>,
    pub(crate) ducklakes: parking_lot::RwLock<Option<Vec<DucklakeConfig>>>,
}

impl ConnectionPool {
    pub fn new(
        db: DbType,
        pool_size: u32,
        timeout: Duration,
        access_mode: AccessMode,
        extensions: &Option<Vec<Extension>>,
        secrets: &Option<Vec<SecretConfig>>,
        ducklakes: &Option<Vec<DucklakeConfig>>,
    ) -> Result<Self> {
        info!(
            "Creating connection pool: db={}, pool_size={}, access_mode={:?}, timeout={:?}",
            db, pool_size, access_mode, timeout
        );

        let inode = match &db {
            DbType::File(path) => Some(std::fs::metadata(path)?.ino()),
            DbType::Memory(_) => None,
        };

        let pool = Self::create_pool(
            &db,
            pool_size,
            timeout,
            &access_mode,
            extensions,
            secrets,
            ducklakes,
        )?;

        Ok(Self {
            db,
            pool_size,
            timeout,
            access_mode,
            pool: parking_lot::RwLock::new(pool),
            inode: parking_lot::RwLock::new(inode),
            extensions: parking_lot::RwLock::new(extensions.clone()),
            secrets: parking_lot::RwLock::new(secrets.clone()),
            ducklakes: parking_lot::RwLock::new(ducklakes.clone()),
        })
    }

    pub fn get(&self) -> Result<r2d2::PooledConnection<DuckdbConnectionManager>, AppError> {
        info!("Checking out connection from pool: db={}", self.db);

        if let DbType::File(path) = &self.db {
            let current_inode = match std::fs::metadata(path) {
                Ok(meta) => meta.ino(),
                Err(e) => {
                    tracing::error!("DuckDB file missing or inaccessible ({}); attempting to rebuild pool", e);
                    self.reset_pool(None).map_err(|e| AppError::Error(e))?;
                    let pool_guard = self.pool.read();
                    return pool_guard.get().map_err(|e| {
                        let err_str = e.to_string().to_lowercase();
                        tracing::error!("Pool error after rebuild: {}", e);
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
                if *upg != Some(current_inode) {
                    info!(
                        "Detected file change (inode = {}). Rebuilding pool for {}",
                        current_inode, self.db
                    );

                    let mut write_guard = parking_lot::RwLockUpgradableReadGuard::upgrade(upg);
                    let (new_pool, _) = self.reset_pool_internal().map_err(|e| AppError::Error(e))?;
                    *self.pool.write() = new_pool;
                    *write_guard = Some(current_inode);
                }
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

        if new_inode.is_some() {
            *self.inode.write() = new_inode;
        }
        else {
            *self.inode.write() = detected_inode;
        }

        Ok(())
    }

    pub(crate) fn reset_pool_internal(&self) -> Result<(r2d2::Pool<DuckdbConnectionManager>, Option<u64>)> {
        let extensions = self.extensions.read();
        let secrets = self.secrets.read();
        let ducklakes = self.ducklakes.read();

        let new_pool = Self::create_pool(
            &self.db,
            self.pool_size,
            self.timeout,
            &self.access_mode,
            &*extensions,
            &*secrets,
            &*ducklakes,
        )?;

        let inode = match &self.db {
            DbType::File(path) => Some(std::fs::metadata(path)?.ino()),
            DbType::Memory(_) => None,
        };

        Ok((new_pool, inode))
    }

    fn create_pool(
        db: &DbType,
        pool_size: u32,
        timeout: Duration,
        access_mode: &AccessMode,
        extensions: &Option<Vec<Extension>>,
        secrets: &Option<Vec<SecretConfig>>,
        ducklakes: &Option<Vec<DucklakeConfig>>,
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

        let manager = match db {
            DbType::File(path) => DuckdbConnectionManager::file_with_flags(path, config)?,
            DbType::Memory(_) => DuckdbConnectionManager::memory_with_flags(config)?,
        };
        let pool = r2d2::Pool::builder()
            .max_size(pool_size)
            .min_idle(Some(1))
            .connection_timeout(timeout)
            .build(manager)?;

        let conn = pool.get()?;

        if let DbType::File(_) = db {
            match conn.execute("checkpoint", []) {
                Ok(_) => tracing::info!("Database {} checkpoint validation successful", db),
                Err(e) => {
                    tracing::error!("Database {} checkpoint validation failed: {}", db, e);
                    return Err(anyhow::anyhow!("Database {} checkpoint validation error: {}", db, e));
                }
            }
        }

        _ = conn.execute_batch(&(AUTOINSTALL_QUERY.join(";")))?;

        if let Some(extensions) = extensions {
            load_extensions(&conn, extensions)?;
        }

        if let Some(secrets) = secrets {
            setup_secrets(&conn, secrets)?;
        }

        if let Some(ducklakes) = ducklakes {
            setup_ducklakes(&conn, ducklakes)?;
        }

        Ok(pool)
    }

    pub fn status(&self) -> Result<PoolStatus, AppError> {
        let pool_guard = self.pool.read();
        let pool_info = pool_guard.state();

        Ok(PoolStatus {
            db_path: self.db.to_string(),
            pool_size: self.pool_size as usize,
            access_mode: format!("{:?}", self.access_mode),
            in_use: (pool_info.connections - pool_info.idle_connections) as usize,
            idle: pool_info.idle_connections as usize,
            total: pool_info.connections as usize,
            timeout: self.timeout,
        })
    }

    pub fn kill_all_connections(&self) -> Result<()> {
        info!("Interrupting all connections in pool: {}", self.db);

        self.reset_pool(None)?;

        Ok(())
    }
}
