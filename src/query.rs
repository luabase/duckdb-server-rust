use std::future::Future;
use std::pin::Pin;

use crate::cache::retrieve;
use crate::interfaces::{AppError, Command, QueryParams, QueryResponse};
use crate::state::AppState;
use tokio::time::{sleep, Duration};

pub async fn with_db_retry<F>(state: &AppState, params: QueryParams, query_fn: F) -> Result<QueryResponse, AppError>
where
    F: for<'a> Fn(
        &'a AppState,
        QueryParams,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResponse, AppError>> + Send + 'a>>,
{
    let mut attempt = 0;
    let max_retries = 3;

    loop {
        attempt += 1;

        match query_fn(state, params.clone()).await {
            Ok(response) => return Ok(response),
            Err(AppError::Timeout) => {
                return Err(AppError::Timeout);
            }
            Err(AppError::Error(err)) => {
                let err_str = err.to_string().to_lowercase();
                if err_str.contains("timeout") || err_str.contains("connection pool timeout") {
                    return Err(AppError::Timeout);
                }
                
                if let Some(duckdb::Error::DuckDBFailure(_, _)) = err.downcast_ref::<duckdb::Error>() {
                    if err_str.contains("stale file handle") 
                    || err_str.contains("write-write conflict") 
                    || err_str.contains("database has been invalidated")
                    || err_str.contains("failed to load metadata pointer") {
                        if attempt <= max_retries {
                            let delay = if attempt == 1 {
                                Duration::from_secs(0)
                            }
                            else {
                                Duration::from_secs(2u64.pow(attempt - 2))
                            };

                            tracing::warn!(
                                "DuckDB failure encountered: {}. Retrying after recreating connection in {:?}. Attempt: {}",
                                err,
                                delay,
                                attempt
                            );

                            sleep(delay).await;

                            state
                                .reconnect_db(params.dynamic_id.as_deref(), &params.database)
                                .await?;

                            continue;
                        }
                        else {
                            tracing::error!("Max retries exceeded for DuckDB failure: {}", err);
                        }
                    }
                }

                return Err(AppError::Error(err));
            }
            Err(err) => return Err(err),
        }
    }
}

pub async fn handle(state: &AppState, params: QueryParams) -> Result<QueryResponse, AppError> {
    let command = &params.query_type;

    let db_state = if let Some(dynamic_id) = &params.dynamic_id {
        state
            .get_or_create_dynamic_db_state(dynamic_id, &params.database)
            .await?
    }
    else {
        state.get_or_create_static_db_state(&params.database).await?
    };

    tracing::info!("Command: '{:?}', Params: '{:?}'", command, params);

    match command {
        Some(Command::Arrow) => {
            if let Some(sql) = params.sql {
                let persist = params.persist.unwrap_or(false);
                let invalidate = params.invalidate.unwrap_or(false);
                let args = params.args.unwrap_or_default();
                let limit = params.limit.unwrap_or(state.defaults.row_limit);
                let prepare_sql = params.prepare_sql;
                let buffer = retrieve(
                    &db_state.cache,
                    sql.clone().as_str(),
                    &args,
                    &Command::Arrow,
                    persist,
                    invalidate,
                    || db_state.db.get_arrow(sql, &args, prepare_sql, limit, params.extensions.as_deref()),
                )
                .await?;
                Ok(QueryResponse::Arrow(buffer))
            }
            else {
                Err(AppError::BadRequest(anyhow::anyhow!("SQL query is required for Arrow command")))
            }
        }
        Some(Command::Exec) => {
            if let Some(sql) = params.sql.as_deref() {
                db_state.db.execute(sql, params.extensions.as_deref()).await?;
                Ok(QueryResponse::Empty)
            }
            else {
                Err(AppError::BadRequest(anyhow::anyhow!("SQL query is required for Exec command")))
            }
        }
        Some(Command::Json) => {
            if let Some(sql) = params.sql {
                let persist = params.persist.unwrap_or(false);
                let invalidate = params.invalidate.unwrap_or(false);
                let args = params.args.unwrap_or_default();
                let limit = params.limit.unwrap_or(state.defaults.row_limit);
                let prepare_sql = params.prepare_sql;
                let json: Vec<u8> = retrieve(&db_state.cache, sql.clone().as_str(), &args, &Command::Json, persist, invalidate, || {
                    db_state.db.get_json(sql, &args, prepare_sql, limit, params.extensions.as_deref())
                })
                .await?;

                let string = if json.is_empty() {
                    "[]".to_string()
                }
                else {
                    String::from_utf8(json)?
                };

                Ok(QueryResponse::Json(string))
            }
            else {
                Err(AppError::BadRequest(anyhow::anyhow!("SQL query is required for Json command")))
            }
        }
        None => Err(AppError::BadRequest(anyhow::anyhow!("Query type is required"))),
    }
}
