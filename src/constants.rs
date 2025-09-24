use git_version::git_version;
use once_cell::sync::Lazy;

#[allow(unused)]
pub const DEFAULT_DB_ID: &str = "default";
#[allow(unused)]
pub const DEFAULT_DB_PATH: &str = ":memory:";
#[allow(unused)]
pub const DEFAULT_CONNECTION_POOL_SIZE: u32 = 10;
#[allow(unused)]
pub const DEFAULT_CACHE_SIZE: usize = 1000;
#[allow(unused)]
pub const GIT_VERSION: &str = git_version!(fallback = env!("GIT_HASH"));
#[allow(unused)]
pub const DEFAULT_ROW_LIMIT: usize = 2000;

#[allow(unused)]
pub const MEMORY_DB_PATH: &str = ":memory:";

pub static FULL_VERSION: Lazy<String> = Lazy::new(|| format!("{} (git {})", env!("CARGO_PKG_VERSION"), GIT_VERSION));

#[allow(unused)]
pub const AUTOINSTALL_QUERY: &str = r#"
INSTALL icu; LOAD icu;
INSTALL json; LOAD json;
INSTALL httpfs; LOAD httpfs;
INSTALL iceberg; LOAD iceberg;
"#;

pub const RETRIABLE_ERRORS: &[&str] = &[
    "stale file handle",
    "write-write conflict",
    "database has been invalidated",
    "failed to attach ducklake metadata"
];

pub const TIMEOUT_ERRORS: &[&str] = &[
    "timeout",
    "connection pool timeout",
];
