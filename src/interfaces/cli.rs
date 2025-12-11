use clap::Parser;
use std::net::{IpAddr, Ipv4Addr};

use crate::constants::{DEFAULT_CACHE_SIZE, DEFAULT_ROW_LIMIT};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub enum Command {
    #[command(about = "Run the DuckDB server")]
    Serve(Args),
    #[command(about = "Print the DuckDB library version")]
    Version,
}

#[derive(Parser, Debug)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Database root path
    #[arg(long = "root", num_args = 1)]
    pub db_root: String,

    /// HTTP Address
    #[arg(short, long, default_value_t = Ipv4Addr::UNSPECIFIED.into())]
    pub address: IpAddr,

    /// HTTP Port
    #[arg(short = 'p', long, default_value_t = 3000)]
    pub http_port: u16,

    /// gRPC Port
    #[arg(short, long, default_value_t = 3030)]
    pub grpc_port: u16,

    /// Request timeout
    #[arg(short, long, default_value_t = 60)]
    pub timeout: u32,

    /// Max connection pool size
    #[arg(long)]
    pub connection_pool_size: Option<u32>,

    /// Max number of cache entries
    #[arg(long, default_value_t = DEFAULT_CACHE_SIZE)]
    pub cache_size: usize,

    /// Database access mode
    #[arg(long, default_value = "automatic")]
    pub access_mode: String,

    /// Default row limit
    #[arg(long, default_value_t = DEFAULT_ROW_LIMIT)]
    pub row_limit: usize,

    /// Connection pool timeout in seconds
    #[arg(long, default_value_t = 10)]
    pub pool_timeout: u64,

    /// Enable authentication
    #[arg(long)]
    pub service_auth_enabled: bool,

    /// Authentication token
    #[arg(long)]
    pub service_auth_token: Option<String>,
}
