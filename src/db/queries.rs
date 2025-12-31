use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use duckdb::{params_from_iter, types::ToSql};
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::interfaces::{AppError, DucklakeConfig, Extension, SecretConfig, SqlValue};
use crate::sql::{enforce_query_limit, is_writable_sql};

use super::config::{
    load_extensions, merge_ducklakes, merge_extensions, merge_secrets,
    setup_ducklakes, setup_secrets,
};
use super::pool::ConnectionPool;
use super::traits::{Database, PoolStatus};

fn catch_query_panic<T, F: FnOnce() -> Result<T>>(sql: &str, f: F) -> Result<T> {
    match panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(panic_info) => {
            let panic_msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = panic_info.downcast_ref::<String>() {
                s.clone()
            } else {
                "Unknown panic".to_string()
            };
            tracing::error!(
                "DuckDB panic caught!\nQuery: {}\nPanic: {}",
                sql,
                panic_msg
            );
            Err(anyhow::anyhow!("Query caused internal error: {}", panic_msg))
        }
    }
}

fn get_duckdb_memory_bytes(conn: &duckdb::Connection) -> i64 {
    conn.prepare("SELECT sum(memory_usage_bytes) FROM duckdb_memory()")
        .and_then(|mut stmt| {
            stmt.query_row([], |row| row.get::<_, i64>(0))
        })
        .unwrap_or(0)
}

#[cfg(target_os = "linux")]
fn get_process_memory_mb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|content| {
            content
                .lines()
                .find(|line| line.starts_with("VmRSS:"))
                .and_then(|line| {
                    line.split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse::<u64>().ok())
                })
        })
        .map(|kb| kb / 1024)
        .unwrap_or(0)
}

#[cfg(target_os = "macos")]
fn get_process_memory_mb() -> u64 {
    use mach2::kern_return::KERN_SUCCESS;
    use mach2::task::task_info;
    use mach2::task_info::{mach_task_basic_info, MACH_TASK_BASIC_INFO, MACH_TASK_BASIC_INFO_COUNT};
    use mach2::traps::mach_task_self;
    use std::mem::MaybeUninit;

    unsafe {
        let mut info = MaybeUninit::<mach_task_basic_info>::uninit();
        let mut count = MACH_TASK_BASIC_INFO_COUNT;

        let result = task_info(
            mach_task_self(),
            MACH_TASK_BASIC_INFO,
            info.as_mut_ptr() as *mut _,
            &mut count,
        );

        if result == KERN_SUCCESS {
            info.assume_init().resident_size / (1024 * 1024)
        } else {
            0
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn get_process_memory_mb() -> u64 {
    0
}

#[async_trait]
impl Database for Arc<ConnectionPool> {
    async fn execute(&self, sql: &str, default_schema: &Option<String>, extensions: &Option<Vec<Extension>>) -> Result<()> {
        let conn = self.get().map_err(|e| anyhow::anyhow!("{}", e))?;

        if let Some(exts) = extensions {
            load_extensions(&conn, exts)?;
        }

        if let Some(default_schema) = default_schema {
            conn.execute_batch(&format!("USE {}", default_schema))?;
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
        default_schema: &Option<String>,
        limit: usize,
        extensions: &Option<Vec<Extension>>,
        secrets: &Option<Vec<SecretConfig>>,
        ducklakes: &Option<Vec<DucklakeConfig>>,
        cancel_token: &CancellationToken,
    ) -> Result<Vec<u8>> {
        let sql_owned = sql.clone();
        let prepare_sql_owned = prepare_sql.clone();
        let effective_sql = enforce_query_limit(&sql_owned, limit)?;
        let args = args.clone().unwrap_or_default();
        let pool = Arc::clone(self);
        let extensions_owned = extensions.clone();
        let secrets_owned = secrets.clone();
        let ducklakes_owned = ducklakes.clone();
        let default_schema_owned = default_schema.clone();

        let result = tokio::select! {
            result = tokio::task::spawn_blocking({
                let cancel_token = cancel_token.clone();
                move || -> Result<Vec<u8>> {
                    catch_query_panic(&effective_sql, || {
                        let conn = pool.get().map_err(|e| anyhow::anyhow!("{}", e))?;

                        if let Some(default_schema) = default_schema_owned {
                            conn.execute_batch(&format!("USE {}", default_schema))?;
                        }

                        if let Some(prepare_sql) = prepare_sql_owned {
                            conn.execute_batch(&prepare_sql)?;
                        }

                        setup_and_merge_configs(
                            &conn,
                            &pool,
                            extensions_owned.as_deref(),
                            secrets_owned.as_deref(),
                            ducklakes_owned.as_deref(),
                        )?;

                        let start = Instant::now();

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

                        let duration_ms = start.elapsed().as_millis() as u64;
                        let process_memory_mb = get_process_memory_mb();
                        let duckdb_memory_bytes = get_duckdb_memory_bytes(&conn);
                        tracing::info!(
                            duration_ms = duration_ms,
                            process_memory_mb = process_memory_mb,
                            duckdb_memory_bytes = duckdb_memory_bytes,
                            sql = %effective_sql,
                            "Query completed"
                        );

                        Ok(writer.into_inner())
                    })
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
        default_schema: &Option<String>,
        limit: usize,
        extensions: &Option<Vec<Extension>>,
        secrets: &Option<Vec<SecretConfig>>,
        ducklakes: &Option<Vec<DucklakeConfig>>,
        cancel_token: &CancellationToken,
    ) -> Result<Vec<u8>> {
        let sql_owned = sql.clone();
        let prepare_sql_owned = prepare_sql.clone();
        let effective_sql = enforce_query_limit(&sql_owned, limit)?;
        let args = args.clone().unwrap_or_default();
        let pool = Arc::clone(self);
        let extensions_owned = extensions.clone();
        let secrets_owned = secrets.clone();
        let ducklakes_owned = ducklakes.clone();
        let default_schema_owned = default_schema.clone();

        let result = tokio::select! {
            result = tokio::task::spawn_blocking({
                let cancel_token = cancel_token.clone();
                move || -> Result<Vec<u8>> {
                    catch_query_panic(&effective_sql, || {
                        let conn = pool.get().map_err(|e| anyhow::anyhow!("{}", e))?;

                        if let Some(default_schema) = default_schema_owned {
                            conn.execute_batch(&format!("USE {}", default_schema))?;
                        }

                        if let Some(prepare_sql) = prepare_sql_owned {
                            conn.execute_batch(&prepare_sql)?;
                        }

                        setup_and_merge_configs(
                            &conn,
                            &pool,
                            extensions_owned.as_deref(),
                            secrets_owned.as_deref(),
                            ducklakes_owned.as_deref(),
                        )?;

                        let start = Instant::now();

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

                        let duration_ms = start.elapsed().as_millis() as u64;
                        let process_memory_mb = get_process_memory_mb();
                        let duckdb_memory_bytes = get_duckdb_memory_bytes(&conn);
                        tracing::info!(
                            duration_ms = duration_ms,
                            process_memory_mb = process_memory_mb,
                            duckdb_memory_bytes = duckdb_memory_bytes,
                            sql = %effective_sql,
                            "Query completed"
                        );

                        Ok(buffer)
                    })
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
        default_schema: &Option<String>,
        limit: usize,
        extensions: &Option<Vec<Extension>>,
        secrets: &Option<Vec<SecretConfig>>,
        ducklakes: &Option<Vec<DucklakeConfig>>,
        cancel_token: &CancellationToken,
    ) -> Result<Vec<RecordBatch>> {
        let sql_owned = sql.clone();
        let effective_sql = enforce_query_limit(&sql_owned, limit)?;
        let args = args.clone().unwrap_or_default();
        let pool = Arc::clone(self);
        let prepare_sql_owned = prepare_sql.clone();
        let extensions_owned = extensions.clone();
        let secrets_owned = secrets.clone();
        let ducklakes_owned = ducklakes.clone();
        let default_schema_owned = default_schema.clone();

        let result = tokio::select! {
            result = tokio::task::spawn_blocking({
                let cancel_token = cancel_token.clone();
                move || -> Result<Vec<RecordBatch>> {
                    catch_query_panic(&effective_sql, || {
                        let conn = pool.get().map_err(|e| anyhow::anyhow!("{}", e))?;

                        if let Some(default_schema) = default_schema_owned {
                            conn.execute_batch(&format!("USE {}", default_schema))?;
                        }

                        if let Some(prepare_sql) = prepare_sql_owned {
                            conn.execute_batch(&prepare_sql)?;
                        }

                        setup_and_merge_configs(
                            &conn,
                            &pool,
                            extensions_owned.as_deref(),
                            secrets_owned.as_deref(),
                            ducklakes_owned.as_deref(),
                        )?;

                        let start = Instant::now();

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

                        let duration_ms = start.elapsed().as_millis() as u64;
                        let process_memory_mb = get_process_memory_mb();
                        let duckdb_memory_bytes = get_duckdb_memory_bytes(&conn);
                        tracing::info!(
                            duration_ms = duration_ms,
                            process_memory_mb = process_memory_mb,
                            duckdb_memory_bytes = duckdb_memory_bytes,
                            sql = %effective_sql,
                            "Query completed"
                        );

                        Ok(batches)
                    })
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
        ConnectionPool::status(self)
    }

    fn kill_all_connections(&self) -> Result<()> {
        ConnectionPool::kill_all_connections(self)
    }
}

fn setup_and_merge_configs(
    conn: &duckdb::Connection,
    pool: &Arc<ConnectionPool>,
    extensions: Option<&[Extension]>,
    secrets: Option<&[SecretConfig]>,
    ducklakes: Option<&[DucklakeConfig]>,
) -> Result<()> {
    if let Some(exts) = extensions {
        load_extensions(conn, exts)?;
        let mut extensions_guard = pool.extensions.write();
        let merged_extensions = merge_extensions(&*extensions_guard, exts);
        *extensions_guard = Some(merged_extensions);
    }

    if let Some(secrets) = secrets {
        setup_secrets(conn, secrets)?;
        let mut secrets_guard = pool.secrets.write();
        let merged_secrets = merge_secrets(&*secrets_guard, secrets);
        *secrets_guard = Some(merged_secrets);
    }

    if let Some(ducklakes) = ducklakes {
        setup_ducklakes(conn, ducklakes)?;
        let mut ducklakes_guard = pool.ducklakes.write();
        let merged_ducklakes = merge_ducklakes(&*ducklakes_guard, ducklakes);
        *ducklakes_guard = Some(merged_ducklakes);
    }

    Ok(())
}
