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
pub const GIT_VERSION: &str = git_version!();

pub static FULL_VERSION: Lazy<String> = Lazy::new(|| {
    format!("{} (git {})", env!("CARGO_PKG_VERSION"), GIT_VERSION)
});
