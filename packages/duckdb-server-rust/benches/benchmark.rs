use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use serde_json::to_value;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::HashMap;

use duckdb_server::{get_key, handle, AppState, Command, ConnectionPool, DbConfig, DbState, QueryParams};

pub fn benchmark(c: &mut Criterion) {
    let config = DbConfig { id: "default".to_string(), path: ":memory:".to_string(), pool_size: 10 };
    let db = ConnectionPool::new(&config.path, config.pool_size).unwrap();
    let cache = lru::LruCache::new(10.try_into().unwrap());

    let mut states = HashMap::new();
    states.insert(
        "default".to_string(),
        DbState {
            config,
            db: Box::new(db),
            cache: Mutex::new(cache),
        },
    );

    let state = Arc::new(AppState {
        states: Mutex::new(states),
    });

    let mut group = c.benchmark_group("handle");
    for command in [Command::Arrow, Command::Json].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(to_value(command).unwrap().to_string()),
            command,
            |b, command| {
                b.to_async(FuturesExecutor).iter(|| {
                    let params = QueryParams {
                        database: "default".to_string(),
                        query_type: Some(command.clone()),
                        sql: Some("SELECT 1 AS foo".to_string()),
                        ..QueryParams::default()
                    };
                    handle(&state, params)
                })
            },
        );
    }
    group.finish();

    c.bench_function("get key", |b| {
        b.iter(|| get_key("SELECT 1", &[], &Command::Arrow))
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
