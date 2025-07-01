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

pub static FULL_VERSION: Lazy<String> = Lazy::new(|| format!("{} (git {})", env!("CARGO_PKG_VERSION"), GIT_VERSION));

#[allow(unused)]
pub const AUTOINSTALL_QUERY: &str = r#"
SET allow_unsigned_extensions=1;
SET autoinstall_known_extensions=1;
SET autoload_known_extensions=1;
INSTALL icu; LOAD icu;
INSTALL json; LOAD json;
INSTALL httpfs; LOAD httpfs;
INSTALL iceberg; LOAD iceberg;
"#;
