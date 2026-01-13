use anyhow::Result;
use duckdb::AccessMode;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::constants::MEMORY_DB_PATH;
use crate::db::ConnectionPool;
use crate::interfaces::{AppError, DbDefaults, DbState, DbType, DucklakeConfig, Extension, SecretConfig};

#[derive(Clone)]
pub struct RunningQuery {
    pub id: String,
    pub cancel_token: CancellationToken,
    pub database: String,
    pub sql: String,
    pub started_at: std::time::SystemTime,
}

pub struct AppState {
    pub defaults: DbDefaults,
    pub root: String,
    pub states: Mutex<HashMap<String, Arc<DbState>>>,
    pub running_queries: Mutex<HashMap<String, RunningQuery>>,
}

impl AppState {
    pub async fn get_or_create_db_state(
        &self,
        database: &str,
        extensions: &Option<Vec<Extension>>,
        secrets: &Option<Vec<SecretConfig>>,
        ducklakes: &Option<Vec<DucklakeConfig>>,
    ) -> Result<Arc<DbState>, AppError> {
        let mut states = self.states.lock().await;

        if let Some(state) = states.get(database) {
            return Ok(Arc::clone(state));
        }

        let db_type = self.resolve_db_type(database)?;
        let access_mode = AppState::convert_access_mode(&self.defaults.access_mode);

        let idle_timeout = if self.defaults.pool_idle_timeout > 0 {
            Some(Duration::from_secs(self.defaults.pool_idle_timeout))
        } else {
            None
        };
        let max_lifetime = if self.defaults.pool_max_lifetime > 0 {
            Some(Duration::from_secs(self.defaults.pool_max_lifetime))
        } else {
            None
        };

        tracing::info!(
            "Creating DuckDB connection: db={}, pool_size={}, access_mode={}, timeout={}, idle_timeout={:?}, max_lifetime={:?}",
            db_type,
            self.defaults.connection_pool_size,
            self.defaults.access_mode,
            self.defaults.pool_timeout,
            idle_timeout,
            max_lifetime
        );

        let db = ConnectionPool::new(
            db_type,
            self.defaults.connection_pool_size,
            Duration::from_secs(self.defaults.pool_timeout),
            idle_timeout,
            max_lifetime,
            access_mode,
            extensions,
            secrets,
            ducklakes,
        )?;

        let cache = Mutex::new(lru::LruCache::new(self.defaults.cache_size.try_into()?));

        let new_state = Arc::new(DbState {
            db: Box::new(Arc::new(db)),
            cache,
        });

        states.insert(database.to_string(), Arc::clone(&new_state));
        Ok(new_state)
    }

    fn resolve_db_type(&self, database: &str) -> Result<DbType, AppError> {
        if database.starts_with(MEMORY_DB_PATH) {
            return Ok(DbType::Memory(
                database
                    .strip_prefix(MEMORY_DB_PATH)
                    .ok_or_else(|| AppError::BadRequest(anyhow::anyhow!("Expected database id to start with {} but got {}", MEMORY_DB_PATH, database).into()))?
                    .to_string())
            );
        }

        let path = PathBuf::from(&self.root).join(database);
        if path.exists() {
            let path_str = path.to_str().ok_or_else(|| {
                AppError::BadRequest(anyhow::anyhow!(
                    "Database path contains invalid UTF-8"
                ).into())
            })?;
            Ok(DbType::File(path_str.to_string()))
        } else {
            Err(AppError::BadRequest(anyhow::anyhow!(
                "Database not found: {}",
                database
            ).into()))
        }
    }

    pub async fn reconnect_db(&self, database: &str) -> Result<(), AppError> {
        let states = self.states.lock().await;

        if let Some(db_state) = states.get(database) {
            db_state.db.reconnect()?;
        }
        else {
            return Err(AppError::BadRequest(anyhow::anyhow!("Database {} not found", database).into()));
        }

        Ok(())
    }

    fn convert_access_mode(mode: &str) -> AccessMode {
        match mode.to_lowercase().as_str() {
            "readwrite" => AccessMode::ReadWrite,
            "readonly" => AccessMode::ReadOnly,
            _ => AccessMode::Automatic,
        }
    }

    pub async fn start_query(&self, database: String, sql: String) -> (String, CancellationToken) {
        let query_id = Uuid::new_v4().to_string();
        let cancel_token = CancellationToken::new();

        let running_query = RunningQuery {
            id: query_id.clone(),
            cancel_token: cancel_token.clone(),
            database: database.clone(),
            sql: sql.clone(),
            started_at: std::time::SystemTime::now(),
        };

        self.running_queries
            .lock()
            .await
            .insert(query_id.clone(), running_query);

        tracing::info!("Started query {} for database {}", query_id, database);
        (query_id, cancel_token)
    }

    pub async fn cancel_query(&self, query_id: &str) -> Result<bool, AppError> {
        let mut queries = self.running_queries.lock().await;

        if let Some(query) = queries.remove(query_id) {
            query.cancel_token.cancel();
            tracing::info!("Cancelled query {} for database {}", query_id, query.database);
            Ok(true)
        }
        else {
            Ok(false)
        }
    }

    pub async fn get_running_queries(&self) -> Vec<RunningQuery> {
        self.running_queries.lock().await.values().cloned().collect()
    }

    pub async fn create_database_if_not_exists(&self, database: &str) -> Result<(), AppError> {
        if database.trim().starts_with(MEMORY_DB_PATH) {
            return Ok(());
        }

        let path = PathBuf::from(&self.root).join(database);
        let access_mode = AppState::convert_access_mode(&self.defaults.access_mode);

        if access_mode == duckdb::AccessMode::ReadOnly {
            return Err(AppError::BadRequest(anyhow::anyhow!(
                "Cannot create database in readonly mode"
            ).into()));
        }

        if !path.exists() {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    AppError::Error(anyhow::anyhow!("Failed to create database directory: {}", e).into())
                })?;
            }

            let conn = duckdb::Connection::open(&path).map_err(|e| {
                AppError::Error(anyhow::anyhow!("Failed to create database: {}", e).into())
            })?;

            drop(conn);

            tracing::info!("Created database file: {}", path.display());
        } else {
            tracing::debug!("Database file already exists: {}", path.display());
        }

        Ok(())
    }
}
