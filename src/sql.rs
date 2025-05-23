use sqlparser::{
    ast::{Expr, Statement, Value},
    dialect::GenericDialect,
    parser::Parser,
};
use tracing::log::info;

pub fn enforce_query_limit(sql: &str, limit: usize) -> anyhow::Result<String> {
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)?;

    for stmt in &mut statements {
        if let Statement::Query(query) = stmt {
            if query.limit.is_none() {
                let original_query = query.to_string();
                query.limit = Some(Expr::Value(Value::Number(limit.to_string(), false).into()));
                let rewritten_query = query.to_string();
                info!("Enforced query limit: original='{}', rewritten='{}'", original_query, rewritten_query);
            }
        }
    }

    Ok(statements
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; "))
}

pub fn is_writable_sql(sql: &str) -> bool {
    let dialect = GenericDialect {};
    match Parser::parse_sql(&dialect, sql) {
        Ok(statements) => statements.iter().any(|stmt| match stmt {
            Statement::Insert { .. }
            | Statement::Update { .. }
            | Statement::Delete { .. }
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
