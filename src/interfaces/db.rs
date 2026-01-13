use std::fmt;
use tokio::sync::Mutex;

use crate::constants::MEMORY_DB_PATH;
use crate::db::Database;

#[derive(Debug, Clone)]
pub struct DbDefaults {
    pub access_mode: String,
    pub cache_size: usize,
    pub connection_pool_size: u32,
    pub row_limit: usize,
    pub pool_timeout: u64,
    pub pool_idle_timeout: u64,
    pub pool_max_lifetime: u64,
}

pub struct DbState {
    pub db: Box<dyn Database>,
    pub cache: Mutex<lru::LruCache<String, Vec<u8>>>,
}

#[derive(Debug, Clone)]
pub enum DbType {
    File(String),
    Memory(String),
}

impl fmt::Display for DbType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbType::File(path) => write!(f, ":file:{}", path),
            DbType::Memory(name) => write!(f, "{}{}", MEMORY_DB_PATH, name),
        }
    }
}