use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use crate::bundle::{create, load};
use crate::cache::retrieve;
use crate::interfaces::{AppError, Command, QueryParams, QueryResponse};
use crate::state::AppState;

fn create_bundle_path(bundle_name: &str) -> PathBuf {
    Path::new(".mosaic").join("bundle").join(bundle_name)
}

pub async fn with_db_retry<F>(state: &AppState, params: QueryParams, query_fn: F) -> Result<QueryResponse, AppError>
where
    F: for<'a> Fn(
        &'a AppState,
        QueryParams,
    ) -> Pin<Box<dyn Future<Output = Result<QueryResponse, AppError>> + Send + 'a>>,
{
    let mut attempt = 0;
    let max_retries = 1; // Define the number of retries.

    loop {
        attempt += 1;

        match query_fn(state, params.clone()).await {
            Ok(response) => return Ok(response),
            Err(AppError::Error(err)) => {
                if let Some(duckdb::Error::DuckDBFailure(_, _)) = err.downcast_ref::<duckdb::Error>() {
                    if err.to_string().to_lowercase().contains("stale file handle") && attempt <= max_retries {
                        tracing::warn!(
                            "DuckDB failure encountered: {}. Retrying after recreating connection. Attempt: {}",
                            err,
                            attempt
                        );
                        state
                            .recreate_db(params.dynamic_id.as_deref(), &params.database)
                            .await?;
                        continue;
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
            if let Some(sql) = params.sql.as_deref() {
                let persist = params.persist.unwrap_or(false);
                let invalidate = params.invalidate.unwrap_or(false);
                let args = params.args.unwrap_or_default();
                let buffer = retrieve(
                    &db_state.cache,
                    sql,
                    &args,
                    &Command::Arrow,
                    persist,
                    invalidate,
                    || db_state.db.get_arrow(sql, &args),
                )
                .await?;
                Ok(QueryResponse::Arrow(buffer))
            }
            else {
                Err(AppError::BadRequest)
            }
        }
        Some(Command::Exec) => {
            if let Some(sql) = params.sql.as_deref() {
                db_state.db.execute(sql).await?;
                Ok(QueryResponse::Empty)
            }
            else {
                Err(AppError::BadRequest)
            }
        }
        Some(Command::Json) => {
            if let Some(sql) = params.sql.as_deref() {
                let persist = params.persist.unwrap_or(false);
                let invalidate = params.invalidate.unwrap_or(false);
                let args = params.args.unwrap_or_default();
                let json: Vec<u8> = retrieve(&db_state.cache, sql, &args, &Command::Json, persist, invalidate, || {
                    db_state.db.get_json(sql, &args)
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
                Err(AppError::BadRequest)
            }
        }
        Some(Command::CreateBundle) => {
            if let Some(queries) = params.queries {
                let bundle_name = params.name.unwrap_or_else(|| "default".to_string());
                let bundle_path = create_bundle_path(&bundle_name);
                create(db_state.db.as_ref(), &db_state.cache, queries, &bundle_path).await?;
                Ok(QueryResponse::Empty)
            }
            else {
                Err(AppError::BadRequest)
            }
        }
        Some(Command::LoadBundle) => {
            if let Some(bundle_name) = params.name {
                let bundle_path = create_bundle_path(&bundle_name);
                load(db_state.db.as_ref(), &db_state.cache, &bundle_path).await?;
                Ok(QueryResponse::Empty)
            }
            else {
                Err(AppError::BadRequest)
            }
        }
        None => Err(AppError::BadRequest),
    }
}
