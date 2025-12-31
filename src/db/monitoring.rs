use std::time::Instant;

pub fn catch_query_panic<T, F: FnOnce() -> anyhow::Result<T>>(sql: &str, f: F) -> anyhow::Result<T> {
    use std::panic::{self, AssertUnwindSafe};

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

pub fn get_duckdb_memory_mb(conn: &duckdb::Connection) -> i64 {
    conn.prepare("SELECT sum(memory_usage_bytes) / 1024 / 1024 FROM duckdb_memory()")
        .and_then(|mut stmt| {
            stmt.query_row([], |row| row.get::<_, i64>(0))
        })
        .unwrap_or(0)
}

pub fn log_query_completed(start: Instant, conn: &duckdb::Connection, sql: &str) {
    let duration_ms = start.elapsed().as_millis() as u64;
    let process_memory_mb = get_process_memory_mb();
    let duckdb_memory_mb = get_duckdb_memory_mb(conn);
    tracing::info!(
        duration_ms = duration_ms,
        process_memory_mb = process_memory_mb,
        duckdb_memory_mb = duckdb_memory_mb,
        sql = %sql,
        "Query completed"
    );
}

#[cfg(target_os = "linux")]
pub fn get_process_memory_mb() -> u64 {
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
pub fn get_process_memory_mb() -> u64 {
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
pub fn get_process_memory_mb() -> u64 {
    0
}

