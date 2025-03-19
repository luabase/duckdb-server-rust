use anyhow::Result;
use duckdb::AccessMode;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::db::{ConnectionPool, Database};
use crate::interfaces::{AppError, DbConfig, DbDefaults, DbPath, DbState};

const AUTOINSTALL_QUERY: &str = r#"
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;
INSTALL icu; LOAD icu;
INSTALL json; LOAD json;
INSTALL httpfs; LOAD httpfs;
INSTALL iceberg; LOAD iceberg;
"#;

pub struct AppState {
    pub defaults: DbDefaults,
    pub paths: HashMap<String, DbPath>,
    pub states: Mutex<HashMap<String, Arc<DbState>>>,
}

impl AppState {
    pub async fn get_or_create_dynamic_db_state(
        &self,
        dynamic: &str,
        database: &str,
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
            return Err(AppError::Error(anyhow::anyhow!(
                "Database ID {} is a static lookup",
                dynamic
            )));
        }

        let path = PathBuf::from(&db_path.path).join(database);
        if path.exists() {
            tracing::info!(
                "Creating DuckDB connection with ID: {}, path: {}, pool size: {}, access_mode: {}",
                id,
                path.display(),
                self.defaults.connection_pool_size,
                self.defaults.access_mode
            );
        }
        else {
            return Err(AppError::Error(anyhow::anyhow!(
                "Database {} not found for ID {} (primary ID {})",
                database,
                dynamic,
                db_path.primary_id
            )));
        }

        let access_mode = AppState::convert_access_mode(&self.defaults.access_mode);

        let db = ConnectionPool::new(path.to_str().unwrap(), self.defaults.connection_pool_size, access_mode)?;
        let cache = Mutex::new(lru::LruCache::new(self.defaults.cache_size.try_into()?));

        db.execute(AUTOINSTALL_QUERY).await?;

        let new_state = Arc::new(DbState {
            config: DbConfig {
                id: id.clone(),
                path: path.to_str().unwrap().to_string(),
                cache_size: self.defaults.cache_size,
                connection_pool_size: self.defaults.connection_pool_size,
            },
            db: Box::new(db),
            cache,
        });

        states.insert(id, Arc::clone(&new_state));
        Ok(new_state)
    }

    pub async fn get_or_create_static_db_state(&self, id: &str) -> Result<Arc<DbState>, AppError> {
        let mut states = self.states.lock().await;

        if let Some(state) = states.get(id) {
            return Ok(Arc::clone(state));
        }

        let db_path = self
            .paths
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("Database ID {} not found", id))?;

        if db_path.is_dynamic {
            return Err(AppError::Error(anyhow::anyhow!(
                "Database ID {} is a dynamic lookup",
                id
            )));
        }

        let effective_pool_size = if db_path.path == ":memory:" {
            1
        }
        else {
            self.defaults.connection_pool_size
        };

        tracing::info!(
            "Creating DuckDB connection with ID: {}, path: {}, pool size: {}, access_mode: {}",
            id,
            db_path.path,
            effective_pool_size,
            self.defaults.access_mode
        );

        let access_mode = AppState::convert_access_mode(&self.defaults.access_mode);
        let db = ConnectionPool::new(&db_path.path, effective_pool_size, access_mode)?;
        let cache = Mutex::new(lru::LruCache::new(self.defaults.cache_size.try_into()?));

        db.execute(AUTOINSTALL_QUERY).await?;

        let new_state = Arc::new(DbState {
            config: DbConfig {
                id: id.to_string(),
                path: db_path.path.clone(),
                cache_size: self.defaults.cache_size,
                connection_pool_size: self.defaults.connection_pool_size,
            },
            db: Box::new(db),
            cache,
        });

        states.insert(id.to_string(), Arc::clone(&new_state));
        Ok(new_state)
    }

    pub async fn recreate_db(&self, dynamic: Option<&str>, database: &str) -> Result<(), AppError> {
        let id = self.get_state_id(dynamic, database)?;
        let mut states = self.states.lock().await;

        if let Some(db_state) = states.get(&id) {
            let config = db_state.config.clone();
            let effective_pool_size = if config.path == ":memory:" {
                1
            }
            else {
                config.connection_pool_size
            };

            let access_mode = AppState::convert_access_mode(&self.defaults.access_mode);
            let db = ConnectionPool::new(&config.path, effective_pool_size, access_mode)?;
            let cache = Mutex::new(lru::LruCache::new(db_state.config.cache_size.try_into()?));

            db.execute(AUTOINSTALL_QUERY).await?;

            tracing::info!(
                "Recreated DuckDB connection with ID: {}, path: {}, pool size: {}, access_mode: {}",
                config.id,
                config.path,
                effective_pool_size,
                self.defaults.access_mode
            );

            let new_state = Arc::new(DbState {
                config,
                db: Box::new(db),
                cache,
            });

            states.insert(id.to_string(), new_state);
        }
        else {
            return Err(AppError::Error(anyhow::anyhow!("Database ID {} not found", id)));
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
}
