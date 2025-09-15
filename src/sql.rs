use sqlparser::{
    ast::{Expr, LimitClause, Statement, Value},
    dialect::DuckDbDialect,
    parser::Parser,
};
use tracing::log::{info, warn};

pub fn enforce_query_limit(sql: &str, limit: usize) -> anyhow::Result<String> {
    let dialect = DuckDbDialect {};
    match Parser::parse_sql(&dialect, sql) {
        Ok(mut statements) => {
            for stmt in &mut statements {
                if let Statement::Query(query) = stmt {
                    if query.limit_clause.is_none() {
                        let original_query = query.to_string();
                        query.limit_clause = Some(LimitClause::LimitOffset {
                            limit: Some(Expr::value(Value::Number(limit.to_string(), false))),
                            offset: None,
                            limit_by: vec![],
                        });
                        let rewritten_query = query.to_string();
                        info!(
                            "Enforced query limit: original='{}', rewritten='{}'",
                            original_query, rewritten_query
                        );
                    }
                }
            }

            Ok(statements
                .into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join("; "))
        }
        Err(e) => {
            warn!("Skipping query limit enforcement due to SQL parse error: {e}. Query: {sql}");
            Ok(sql.to_string())
        }
    }
}

pub fn is_writable_sql(sql: &str) -> bool {
    let dialect = DuckDbDialect {};
    match Parser::parse_sql(&dialect, sql) {
        Ok(statements) => statements.iter().any(|stmt| match stmt {
            Statement::Insert { .. }
            | Statement::Update { .. }
            | Statement::Delete { .. }
            | Statement::CreateSchema { .. }
            | Statement::CreateTable { .. }
            | Statement::CreateView { .. }
            | Statement::CreateIndex { .. }
            | Statement::Drop { .. }
            | Statement::AlterTable { .. }
            | Statement::Copy { .. }
            | Statement::Truncate { .. }
            | Statement::Merge { .. }
            | Statement::Grant { .. }
            | Statement::Revoke { .. } => true,
            Statement::Query(query) => query.with.as_ref().is_some_and(|with| {
                with.cte_tables.iter().any(|cte| {
                    matches!(
                        cte.query.body.as_ref(),
                        sqlparser::ast::SetExpr::Insert { .. } | sqlparser::ast::SetExpr::Update { .. }
                    )
                })
            }),
            _ => false,
        }),
        Err(_) => false,
    }
}
