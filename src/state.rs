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
use crate::interfaces::{AppError, DbDefaults, DbPath, DbState, DucklakeConfig, SecretConfig};

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
    pub paths: HashMap<String, DbPath>,
    pub states: Mutex<HashMap<String, Arc<DbState>>>,
    pub running_queries: Mutex<HashMap<String, RunningQuery>>,
}

impl AppState {
    pub async fn get_or_create_dynamic_db_state(
        &self,
        dynamic: &str,
        database: &str,
        secrets: &Option<Vec<SecretConfig>>,
        ducklake_config: &Option<DucklakeConfig>,
    ) -> Result<Arc<DbState>, AppError> {
        let db_path = self
            .paths
            .get(dynamic)
            .ok_or_else(|| anyhow::anyhow!("Database ID {} not found", dynamic))?;

        let id = self.get_state_id(Some(dynamic), database)?;
        let mut states = self.states.lock().await;

        if let Some(state) = states.get(&id) {
            return Ok(Arc::clone(state));
        }

        if !db_path.is_dynamic {
            return Err(AppError::BadRequest(anyhow::anyhow!(
                "Database ID {} is a static lookup",
                dynamic
            )));
        }

        let path = PathBuf::from(&db_path.path).join(database);
        if path.exists() {
            tracing::info!(
                "Creating DuckDB connection with ID: {}, path: {}, pool size: {}, access_mode: {}, timeout: {}",
                id,
                path.display(),
                self.defaults.connection_pool_size,
                self.defaults.access_mode,
                self.defaults.pool_timeout
            );
        }
        else {
            return Err(AppError::BadRequest(anyhow::anyhow!(
                "Database {} not found for ID {} (primary ID {})",
                database,
                dynamic,
                db_path.primary_id
            )));
        }

        let access_mode = AppState::convert_access_mode(&self.defaults.access_mode);

        let db = ConnectionPool::new(
            path.to_str().unwrap(),
            self.defaults.connection_pool_size,
            Duration::from_secs(self.defaults.pool_timeout),
            access_mode,
            secrets,
            ducklake_config,
        )?;

        let cache = Mutex::new(lru::LruCache::new(self.defaults.cache_size.try_into()?));

        let new_state = Arc::new(DbState {
            db: Box::new(Arc::new(db)),
            cache,
        });

        states.insert(id, Arc::clone(&new_state));
        Ok(new_state)
    }

    pub async fn get_or_create_static_db_state(
        &self, 
        id: &str, 
        secrets: &Option<Vec<SecretConfig>>, 
        ducklake_config: &Option<DucklakeConfig>
    ) -> Result<Arc<DbState>, AppError> {
        let mut states = self.states.lock().await;

        if let Some(state) = states.get(id) {
            return Ok(Arc::clone(state));
        }

        let db_path = self
            .paths
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Database ID {} not found", id))?;

        if db_path.is_dynamic {
            return Err(AppError::BadRequest(anyhow::anyhow!(
                "Database ID {} is a dynamic lookup",
                id
            )));
        }

        let effective_pool_size = if db_path.path == MEMORY_DB_PATH {
            1
        }
        else {
            self.defaults.connection_pool_size
        };

        tracing::info!(
            "Creating DuckDB connection with ID: {}, path: {}, pool size: {}, access_mode: {}, timeout: {}",
            id,
            db_path.path,
            effective_pool_size,
            self.defaults.access_mode,
            self.defaults.pool_timeout
        );

        let access_mode = AppState::convert_access_mode(&self.defaults.access_mode);
        let db = ConnectionPool::new(
            &db_path.path,
            effective_pool_size,
            Duration::from_secs(self.defaults.pool_timeout),
            access_mode,
            secrets,
            ducklake_config,
        )?;

        let cache = Mutex::new(lru::LruCache::new(self.defaults.cache_size.try_into()?));

        let new_state = Arc::new(DbState {
            db: Box::new(Arc::new(db)),
            cache,
        });

        states.insert(id.to_string(), Arc::clone(&new_state));
        Ok(new_state)
    }

    pub async fn reconnect_db(&self, dynamic: Option<&str>, database: &str) -> Result<(), AppError> {
        let id = self.get_state_id(dynamic, database)?;
        let states = self.states.lock().await;

        if let Some(db_state) = states.get(&id) {
            db_state.db.reconnect()?;
        }
        else {
            return Err(AppError::BadRequest(anyhow::anyhow!("Database ID {} not found", id)));
        }

        Ok(())
    }

    fn get_state_id(&self, dynamic: Option<&str>, database: &str) -> Result<String, AppError> {
        let id = if let Some(dynamic_id) = dynamic {
            let db_path = self
                .paths
                .get(dynamic_id)
                .ok_or_else(|| anyhow::anyhow!("Database ID {} not found", dynamic_id))?;
            format!("{}::{}", db_path.primary_id, database)
        }
        else {
            database.to_string()
        };

        Ok(id)
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

    pub async fn create_database_if_not_exists(&self, dynamic_id: &str, database: &str) -> Result<(), AppError> {
        if database.trim() == MEMORY_DB_PATH {
            return Ok(());
        }

        let path = if let Some(db_path) = self.paths.get(dynamic_id) {
            if db_path.is_dynamic {
                PathBuf::from(&db_path.path).join(database)
            } else {
                return Err(AppError::BadRequest(anyhow::anyhow!("Database ID {} is a static lookup", dynamic_id)));
            }
        } else {
            return Err(AppError::BadRequest(anyhow::anyhow!("Database ID {} not found", dynamic_id)));
        };

        let access_mode = AppState::convert_access_mode(&self.defaults.access_mode);

        if access_mode == duckdb::AccessMode::ReadOnly {
            return Err(AppError::BadRequest(anyhow::anyhow!(
                "Cannot create database in readonly mode"
            )));
        }
        
        if !path.exists() {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    AppError::Error(anyhow::anyhow!("Failed to create directory {}: {}", parent.display(), e))
                })?;
            }

            let conn = duckdb::Connection::open(&path).map_err(|e| {
                AppError::Error(anyhow::anyhow!("Failed to create database {}: {}", path.display(), e))
            })?;
            
            drop(conn);
            
            tracing::info!("Created database file: {}", path.display());
        } else {
            tracing::debug!("Database file already exists: {}", path.display());
        }

        Ok(())
    }
}
