use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::db::{ConnectionPool, Database};
use crate::interfaces::{AppError, DbConfig, DbDefaults, DbPath, DbState};

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
        let id = format!("{}::{}", dynamic, database);
        let mut states = self.states.lock().await;

        if let Some(state) = states.get(&id) {
            return Ok(Arc::clone(state));
        }

        let db_path = self
            .paths
            .get(dynamic)
            .ok_or_else(|| anyhow::anyhow!("Database ID {} not found", dynamic))?;

        if !db_path.is_dynamic {
            return Err(AppError::Error(anyhow::anyhow!(
                "Database ID {} is a static lookup",
                dynamic
            )));
        }

        let path = PathBuf::from(&db_path.path).join(database);
        tracing::info!("Creating DuckDB connection for: {}", path.display());

        let db = ConnectionPool::new(path.to_str().unwrap(), self.defaults.connection_pool_size)?;
        let cache = Mutex::new(lru::LruCache::new(self.defaults.cache_size.try_into()?));

        db.execute("INSTALL icu; LOAD icu;").await?;

        let new_state = Arc::new(DbState {
            config: DbConfig {
                id: id.to_string(),
                path: path.to_str().unwrap().to_string(),
                cache_size: self.defaults.cache_size,
                connection_pool_size: self.defaults.connection_pool_size,
            },
            db: Box::new(db),
            cache,
        });

        states.insert(id.to_string(), Arc::clone(&new_state));
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

        tracing::info!("Creating DuckDB connection for: {}", id);

        let effective_pool_size = if db_path.path == ":memory:" {
            1
        }
        else {
            self.defaults.connection_pool_size
        };

        let db = ConnectionPool::new(&db_path.path, effective_pool_size)?;
        let cache = Mutex::new(lru::LruCache::new(self.defaults.cache_size.try_into()?));

        db.execute("INSTALL icu; LOAD icu;").await?;

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

    pub async fn recreate_db(&self, dynamic: Option<&str>, database: &str) -> Result<()> {
        let id = if let Some(dynamic_id) = dynamic {
            format!("{}::{}", dynamic_id, database)
        }
        else {
            database.to_string()
        };

        let mut states = self.states.lock().await;

        if let Some(db_state) = states.get(&id) {
            let config = db_state.config.clone();
            let effective_pool_size = if config.path == ":memory:" {
                1
            }
            else {
                config.connection_pool_size
            };

            let db = ConnectionPool::new(&config.path, effective_pool_size)?;
            let cache = Mutex::new(lru::LruCache::new(db_state.config.cache_size.try_into()?));

            db.execute("INSTALL icu; LOAD icu;").await?;

            tracing::info!("Recreated DuckDB with ID: {}, Path: {}", config.id, config.path);

            let new_state = Arc::new(DbState {
                config,
                db: Box::new(db),
                cache,
            });

            states.insert(id.to_string(), new_state);
        }
        else {
            return Err(anyhow::anyhow!("Database ID {} not found", id));
        }

        Ok(())
    }
}
