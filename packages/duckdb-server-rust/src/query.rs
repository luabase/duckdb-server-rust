use anyhow::Result;
use std::path::{Path, PathBuf};

use crate::bundle::{create, load};
use crate::cache::retrieve;
use crate::interfaces::{AppError, AppState, Command, QueryParams, QueryResponse};

fn create_bundle_path(bundle_name: &str) -> PathBuf {
    Path::new(".mosaic").join("bundle").join(bundle_name)
}

pub async fn handle(state: &AppState, params: QueryParams) -> Result<QueryResponse, AppError> {
    let command = &params.query_type;
    let database_id = &params.database;

    let states = state.states.lock().await;
    let db_state = states.get(database_id).ok_or(AppError::BadRequest)?;

    tracing::info!("Command: '{:?}', Params: '{:?}'", command, params);

    match command {
        Some(Command::Arrow) => {
            if let Some(sql) = params.sql.as_deref() {
                let persist = params.persist.unwrap_or(true);
                let invalidate = params.invalidate.unwrap_or(false);
                let args = params.args.as_deref().unwrap_or(&[]);
                let buffer = retrieve(&db_state.cache, sql, args, &Command::Arrow, persist, invalidate, || {
                    db_state.db.get_arrow(sql, args)
                })
                .await?;
                Ok(QueryResponse::Arrow(buffer))
            } else {
                Err(AppError::BadRequest)
            }
        }
        Some(Command::Exec) => {
            if let Some(sql) = params.sql.as_deref() {
                db_state.db.execute(sql).await?;
                Ok(QueryResponse::Empty)
            } else {
                Err(AppError::BadRequest)
            }
        }
        Some(Command::Json) => {
            if let Some(sql) = params.sql.as_deref() {
                let persist = params.persist.unwrap_or(true);
                let invalidate = params.invalidate.unwrap_or(false);
                let args = params.args.as_deref().unwrap_or(&[]);
                let json: Vec<u8> = retrieve(&db_state.cache, sql, args, &Command::Json, persist, invalidate, || {
                    db_state.db.get_json(sql, args)
                })
                .await?;

                let string = if json.is_empty() {
                    "[]".to_string()
                } else {
                    String::from_utf8(json)?
                };

                Ok(QueryResponse::Json(string))
            } else {
                Err(AppError::BadRequest)
            }
        }
        Some(Command::CreateBundle) => {
            if let Some(queries) = params.queries {
                let bundle_name = params.name.unwrap_or_else(|| "default".to_string());
                let bundle_path = create_bundle_path(&bundle_name);
                create(db_state.db.as_ref(), &db_state.cache, queries, &bundle_path).await?;
                Ok(QueryResponse::Empty)
            } else {
                Err(AppError::BadRequest)
            }
        }
        Some(Command::LoadBundle) => {
            if let Some(bundle_name) = params.name {
                let bundle_path = create_bundle_path(&bundle_name);
                load(db_state.db.as_ref(), &db_state.cache, &bundle_path).await?;
                Ok(QueryResponse::Empty)
            } else {
                Err(AppError::BadRequest)
            }
        }
        None => Err(AppError::BadRequest),
    }
}
