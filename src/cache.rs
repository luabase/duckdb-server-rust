use anyhow::Result;
use serde_json::to_value;
use tokio::sync::Mutex;

use crate::interfaces::{Command, SqlValue};

#[must_use]
pub fn get_key(sql: &str, args: &[SqlValue], command: &Command) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(sql);

    for arg in args {
        let arg_str = format!("{:?}", arg);
        hasher.update(arg_str);
    }

    format!(
        "{:x}.{}",
        hasher.finalize(),
        to_value(command).unwrap().as_str().unwrap()
    )
}

pub async fn retrieve<F, Fut>(
    cache: &Mutex<lru::LruCache<String, Vec<u8>>>,
    sql: &str,
    args: &[SqlValue],
    command: &Command,
    persist: bool,
    invalidate: bool,
    f: F,
) -> Result<Vec<u8>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<Vec<u8>>>,
{
    let key = get_key(sql, args, command);

    if invalidate {
        flush(cache, sql, args, command).await;
    }
    else if let Some(cached) = cache.lock().await.get(&key) {
        tracing::debug!("Cache hit {}!", key);
        return Ok(cached.clone());
    }

    let result = f().await?;

    if persist {
        cache.lock().await.put(key, result.clone());
    }

    Ok(result)
}

pub async fn flush(cache: &Mutex<lru::LruCache<String, Vec<u8>>>, sql: &str, args: &[SqlValue], command: &Command) {
    let key = get_key(sql, args, command);

    let mut cache_lock = cache.lock().await;
    if cache_lock.pop(&key).is_some() {
        tracing::info!("Cache entry cleared for key: {}", key);
    }
    else {
        tracing::info!("No cache entry found for key: {}", key);
    }
}
