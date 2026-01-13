use anyhow::Result;
use duckdb::{AccessMode, Config, Connection, DuckdbConnectionManager};
use std::os::unix::fs::MetadataExt;
use std::time::Duration;
use tracing::log::info;

use crate::constants::AUTOINSTALL_QUERY;
use crate::interfaces::{AppError, DbType, DucklakeConfig, Extension, SecretConfig};

use super::config::{load_extensions, setup_ducklakes, setup_secrets};
use super::instance_cache::CachedConnectionManager;
use super::traits::PoolStatus;

pub(crate) enum PoolType {
    File(r2d2::Pool<DuckdbConnectionManager>),
    Memory(r2d2::Pool<CachedConnectionManager>),
}

impl PoolType {
    fn get(&self) -> Result<PooledConnection, r2d2::Error> {
        match self {
            PoolType::File(pool) => pool.get().map(PooledConnection::File),
            PoolType::Memory(pool) => pool.get().map(PooledConnection::Memory),
        }
    }

    fn state(&self) -> r2d2::State {
        match self {
            PoolType::File(pool) => pool.state(),
            PoolType::Memory(pool) => pool.state(),
        }
    }
}

pub enum PooledConnection {
    File(r2d2::PooledConnection<DuckdbConnectionManager>),
    Memory(r2d2::PooledConnection<CachedConnectionManager>),
}

impl std::ops::Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        match self {
            PooledConnection::File(conn) => conn,
            PooledConnection::Memory(conn) => conn,
        }
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            PooledConnection::File(conn) => conn,
            PooledConnection::Memory(conn) => conn,
        }
    }
}

pub struct ConnectionPool {
    pub(crate) db: DbType,
    pub(crate) pool_size: u32,
    pub(crate) timeout: Duration,
    pub(crate) idle_timeout: Option<Duration>,
    pub(crate) max_lifetime: Option<Duration>,
    pub(crate) access_mode: AccessMode,
    pub(crate) pool: parking_lot::RwLock<PoolType>,
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
        idle_timeout: Option<Duration>,
        max_lifetime: Option<Duration>,
        access_mode: AccessMode,
        extensions: &Option<Vec<Extension>>,
        secrets: &Option<Vec<SecretConfig>>,
        ducklakes: &Option<Vec<DucklakeConfig>>,
    ) -> Result<Self> {
        info!(
            "Creating connection pool: db={}, pool_size={}, access_mode={:?}, timeout={:?}, idle_timeout={:?}, max_lifetime={:?}",
            db, pool_size, access_mode, timeout, idle_timeout, max_lifetime
        );

        let inode = match &db {
            DbType::File(path) => Some(std::fs::metadata(path)?.ino()),
            DbType::Memory(_) => None,
        };

        let pool = Self::create_pool(
            &db,
            pool_size,
            timeout,
            idle_timeout,
            max_lifetime,
            &access_mode,
            extensions,
            secrets,
            ducklakes,
        )?;

        Ok(Self {
            db,
            pool_size,
            timeout,
            idle_timeout,
            max_lifetime,
            access_mode,
            pool: parking_lot::RwLock::new(pool),
            inode: parking_lot::RwLock::new(inode),
            extensions: parking_lot::RwLock::new(extensions.clone()),
            secrets: parking_lot::RwLock::new(secrets.clone()),
            ducklakes: parking_lot::RwLock::new(ducklakes.clone()),
        })
    }

    pub fn get(&self) -> Result<PooledConnection, AppError> {
        info!("Checking out connection from pool: db={}", self.db);

        if let DbType::File(path) = &self.db {
            let current_inode = match std::fs::metadata(path) {
                Ok(meta) => meta.ino(),
                Err(e) => {
                    tracing::error!("DuckDB file missing or inaccessible ({}); attempting to rebuild pool", e);
                    self.reset_pool(None).map_err(|e| AppError::Error(e.into()))?;
                    let pool_guard = self.pool.read();
                    return pool_guard.get().map_err(|e| {
                        let err_str = e.to_string().to_lowercase();
                        tracing::error!("Pool error after rebuild: {}", e);
                        if err_str.contains("timeout") {
                            AppError::Timeout
                        }
                        else {
                            AppError::Error(anyhow::anyhow!("Pool error: {}", e).into())
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
                    let (new_pool, _) = self.reset_pool_internal().map_err(|e| AppError::Error(e.into()))?;
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
                AppError::Error(anyhow::anyhow!("Pool error: {}", e).into())
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

    pub(crate) fn reset_pool_internal(&self) -> Result<(PoolType, Option<u64>)> {
        let extensions = self.extensions.read();
        let secrets = self.secrets.read();
        let ducklakes = self.ducklakes.read();

        let new_pool = Self::create_pool(
            &self.db,
            self.pool_size,
            self.timeout,
            self.idle_timeout,
            self.max_lifetime,
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
        idle_timeout: Option<Duration>,
        max_lifetime: Option<Duration>,
        access_mode: &AccessMode,
        extensions: &Option<Vec<Extension>>,
        secrets: &Option<Vec<SecretConfig>>,
        ducklakes: &Option<Vec<DucklakeConfig>>,
    ) -> Result<PoolType> {
        match db {
            DbType::File(path) => {
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

                let manager = DuckdbConnectionManager::file_with_flags(path, config)?;
                let pool = r2d2::Pool::builder()
                    .max_size(pool_size)
                    .min_idle(Some(1))
                    .connection_timeout(timeout)
                    .idle_timeout(idle_timeout)
                    .max_lifetime(max_lifetime)
                    .build(manager)?;

                let conn = pool.get()?;

                match conn.execute("checkpoint", []) {
                    Ok(_) => tracing::info!("Database {} checkpoint validation successful", db),
                    Err(e) => {
                        tracing::error!("Database {} checkpoint validation failed: {}", db, e);
                        return Err(anyhow::anyhow!("Database {} checkpoint validation error: {}", db, e));
                    }
                }

                Self::init_connection(&conn, extensions, secrets, ducklakes)?;
                Ok(PoolType::File(pool))
            }
            DbType::Memory(id) => {
                tracing::info!("Creating in-memory DuckDB connection with id: {} using instance cache", id);
                let path = format!(":memory:{}", id);
                let manager = CachedConnectionManager::new(path, access_mode.clone(), pool_size);

                let pool = r2d2::Pool::builder()
                    .max_size(pool_size)
                    .min_idle(Some(1))
                    .connection_timeout(timeout)
                    .idle_timeout(idle_timeout)
                    .max_lifetime(max_lifetime)
                    .build(manager)?;

                let conn = pool.get()?;
                Self::init_connection(&conn, extensions, secrets, ducklakes)?;
                Ok(PoolType::Memory(pool))
            }
        }
    }

    fn init_connection(
        conn: &Connection,
        extensions: &Option<Vec<Extension>>,
        secrets: &Option<Vec<SecretConfig>>,
        ducklakes: &Option<Vec<DucklakeConfig>>,
    ) -> Result<()> {
        _ = conn.execute_batch(&(AUTOINSTALL_QUERY.join(";")))?;

        if let Some(extensions) = extensions {
            load_extensions(conn, extensions)?;
        }

        if let Some(secrets) = secrets {
            setup_secrets(conn, secrets)?;
        }

        if let Some(ducklakes) = ducklakes {
            setup_ducklakes(conn, ducklakes)?;
        }

        Ok(())
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
