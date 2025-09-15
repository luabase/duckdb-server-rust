use std::future::Future;
use std::pin::Pin;

use crate::cache::retrieve;
use crate::interfaces::{AppError, Command, QueryInfo, QueryParams, QueryResponse};
use crate::state::AppState;
use tokio::time::{Duration, sleep};

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
                    {
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
    if command.is_none() {
        return Err(AppError::BadRequest(anyhow::anyhow!("Query type is required")));
    }

    let sql = params.sql.clone().ok_or_else(|| {
        AppError::BadRequest(anyhow::anyhow!("SQL query is required"))
    })?;
    
    if sql.trim().is_empty() {
        return Err(AppError::BadRequest(anyhow::anyhow!(
            "SQL query cannot be empty"
        )));
    }

    let db_state = if let Some(dynamic_id) = &params.dynamic_id {
        state
            .get_or_create_dynamic_db_state(dynamic_id, &params.database)
            .await?
    }
    else {
        state.get_or_create_static_db_state(&params.database).await?
    };

    let (query_id, cancel_token) = state.start_query(params.database.clone(), sql.clone()).await;

    tracing::info!(
        "Command: '{:?}', Query ID: '{}', Params: '{:?}'",
        command,
        query_id,
        params
    );

    let result = match command {
        Some(Command::Arrow) => {
            let persist = params.persist.unwrap_or(false);
            let invalidate = params.invalidate.unwrap_or(false);
            let args = params.args.unwrap_or_default();
            let limit = params.limit.unwrap_or(state.defaults.row_limit);
            let prepare_sql = params.prepare_sql;
            let buffer = retrieve(
                &db_state.cache,
                sql.as_str(),
                &args,
                &Command::Arrow,
                persist,
                invalidate,
                || {
                    db_state.db.get_arrow(
                        sql.clone(),
                        &args,
                        prepare_sql,
                        limit,
                        params.extensions.as_deref(),
                        cancel_token.clone(),
                    )
                },
            )
            .await?;
            Ok(QueryResponse::Arrow(buffer))
        }
        Some(Command::Exec) => {
            db_state.db.execute(sql.as_str(), params.extensions.as_deref()).await?;
            Ok(QueryResponse::Empty)
        }
        Some(Command::Json) => {
            let persist = params.persist.unwrap_or(false);
            let invalidate = params.invalidate.unwrap_or(false);
            let args = params.args.unwrap_or_default();
            let limit = params.limit.unwrap_or(state.defaults.row_limit);
            let prepare_sql = params.prepare_sql;
            let json: Vec<u8> = retrieve(
                &db_state.cache,
                sql.as_str(),
                &args,
                &Command::Json,
                persist,
                invalidate,
                || {
                    db_state.db.get_json(
                        sql.clone(),
                        &args,
                        prepare_sql,
                        limit,
                        params.extensions.as_deref(),
                        cancel_token.clone(),
                    )
                },
            )
            .await?;

            let string = if json.is_empty() {
                "[]".to_string()
            }
            else {
                String::from_utf8(json)?
            };

            Ok(QueryResponse::Json(string))
        }
        None => unreachable!("HOLY MOLLY, this should never happen: query type is required"),
    };

    let final_result = match result {
        Ok(response) => Ok(QueryResponse::QueryWithId {
            query_id: query_id.clone(),
            result: Box::new(response),
        }),
        Err(e) => Err(e),
    };

    {
        let mut queries = state.running_queries.lock().await;
        queries.remove(&query_id);
    }

    final_result
}


pub async fn cancel_query(state: &AppState, query_id: String) -> Result<QueryResponse, AppError> {
    let cancelled = state.cancel_query(&query_id).await?;

    if cancelled {
        Ok(QueryResponse::QueryCancelled { query_id })
    }
    else {
        Err(AppError::BadRequest(anyhow::anyhow!("Query not found: {}", query_id)))
    }
}

pub async fn list_running_queries(state: &AppState) -> Result<QueryResponse, AppError> {
    let running_queries = state.get_running_queries().await;

    let query_infos: Vec<QueryInfo> = running_queries
        .into_iter()
        .map(|q| QueryInfo {
            id: q.id,
            database: q.database,
            sql: q.sql,
            started_at: q
                .started_at
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .to_string(),
        })
        .collect();

    Ok(QueryResponse::RunningQueries { queries: query_infos })
}

pub async fn interrupt_all_connections(state: &AppState) -> Result<QueryResponse, AppError> {
    let running_queries = state.get_running_queries().await;
    let mut cancelled_count = 0;

    for query in running_queries {
        if state.cancel_query(&query.id).await? {
            cancelled_count += 1;
        }
    }

    let states = state.states.lock().await;
    for (_, db_state) in states.iter() {
        if let Err(e) = db_state.db.interrupt_all_connections() {
            tracing::warn!("Failed to interrupt connections for database: {}", e);
        }
    }

    let response = serde_json::json!({
        "status": "interrupted",
        "cancelled_queries": cancelled_count,
        "message": "All connections have been interrupted"
    });

    Ok(QueryResponse::Json(response.to_string()))
}
