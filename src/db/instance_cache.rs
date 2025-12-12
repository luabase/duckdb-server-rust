use std::ffi::{CStr, CString};
use std::ptr;
use std::sync::OnceLock;

use duckdb::{AccessMode, Connection};
use libduckdb_sys::{
    duckdb_config, duckdb_create_config, duckdb_create_instance_cache, duckdb_database,
    duckdb_destroy_config, duckdb_destroy_instance_cache, duckdb_free,
    duckdb_get_or_create_from_cache, duckdb_instance_cache, duckdb_set_config,
    duckdb_state_DuckDBError,
};
use parking_lot::Mutex;

static INSTANCE_CACHE: OnceLock<InstanceCache> = OnceLock::new();

fn set_config(config: duckdb_config, key: &str, value: &str) {
    let c_key = CString::new(key).unwrap();
    let c_value = CString::new(value).unwrap();
    unsafe {
        duckdb_set_config(config, c_key.as_ptr(), c_value.as_ptr());
    }
}

pub struct InstanceCache {
    cache: Mutex<duckdb_instance_cache>,
}

unsafe impl Send for InstanceCache {}
unsafe impl Sync for InstanceCache {}

impl InstanceCache {
    fn new() -> Self {
        let cache = unsafe { duckdb_create_instance_cache() };
        Self {
            cache: Mutex::new(cache),
        }
    }

    pub fn global() -> &'static InstanceCache {
        INSTANCE_CACHE.get_or_init(InstanceCache::new)
    }

    pub fn open_connection(
        &self,
        path: &str,
        access_mode: AccessMode,
        threads: u32,
    ) -> Result<Connection, duckdb::Error> {
        let c_path = CString::new(path).map_err(|e| {
            duckdb::Error::InvalidParameterName(e.to_string())
        })?;

        let mut config: duckdb_config = ptr::null_mut();
        let state = unsafe { duckdb_create_config(&mut config) };
        if state == duckdb_state_DuckDBError {
            return Err(duckdb::Error::InvalidParameterName(
                "Failed to create DuckDB config".to_string(),
            ));
        }

        let access_mode_str = match access_mode {
            AccessMode::ReadOnly => "read_only",
            AccessMode::ReadWrite => "read_write",
            AccessMode::Automatic => "automatic",
        };
        set_config(config, "access_mode", access_mode_str);
        set_config(config, "allow_unsigned_extensions", "true");
        set_config(config, "autoinstall_known_extensions", "true");
        set_config(config, "autoload_known_extensions", "true");
        set_config(config, "enable_object_cache", "true");
        set_config(config, "threads", &threads.to_string());

        // Get or create database from cache
        let db = {
            let cache = self.cache.lock();

            let mut db: duckdb_database = ptr::null_mut();
            let mut error: *mut i8 = ptr::null_mut();

            let state = unsafe {
                duckdb_get_or_create_from_cache(*cache, c_path.as_ptr(), &mut db, config, &mut error)
            };

            unsafe {
                duckdb_destroy_config(&mut { config });
            }

            if state == duckdb_state_DuckDBError {
                let error_msg = if !error.is_null() {
                    let msg = unsafe { CStr::from_ptr(error) }
                        .to_string_lossy()
                        .into_owned();
                    unsafe { duckdb_free(error as *mut _) };
                    msg
                } else {
                    "Unknown error opening database".to_string()
                };
                return Err(duckdb::Error::InvalidParameterName(error_msg));
            }

            db
        };

        let connection = unsafe { Connection::open_from_raw(db)? };

        Ok(connection)
    }
}

impl Drop for InstanceCache {
    fn drop(&mut self) {
        let mut cache = self.cache.lock();
        unsafe {
            duckdb_destroy_instance_cache(&mut *cache);
        }
    }
}

pub struct CachedConnectionManager {
    path: String,
    access_mode: AccessMode,
    threads: u32,
}

impl CachedConnectionManager {
    pub fn new(path: String, access_mode: AccessMode, threads: u32) -> Self {
        Self { path, access_mode, threads }
    }
}

impl r2d2::ManageConnection for CachedConnectionManager {
    type Connection = Connection;
    type Error = duckdb::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        InstanceCache::global().open_connection(&self.path, self.access_mode.clone(), self.threads)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.execute_batch("SELECT 1")
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}
