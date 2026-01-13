use regex::Regex;
use std::io::Write;
use std::sync::LazyLock;

static CREDENTIAL_PATTERNS: LazyLock<Vec<(Regex, &'static str)>> = LazyLock::new(|| {
    vec![
        // Database connection strings with credentials (postgresql, postgres, mysql, mongodb, etc.)
        // Matches: protocol://user:password@host
        (
            Regex::new(r"(?i)((?:postgresql|postgres|mysql|mongodb|redis|amqp|mssql)://[^:]+:)[^@]+(@)").unwrap(),
            "${1}[REDACTED]${2}",
        ),
        // Generic URL with credentials pattern
        // Matches: ://user:password@ in any URL
        (
            Regex::new(r"(://[^/:@]+:)[^@]+(@[^/\s]+)").unwrap(),
            "${1}[REDACTED]${2}",
        ),
        // AWS Access Key ID (starts with AKIA, ABIA, ACCA, ASIA)
        (
            Regex::new(r"(?i)((?:aws_access_key_id|access_key_id|accesskeyid)[=:\s]+)(?:AKIA|ABIA|ACCA|ASIA)[A-Z0-9]{16}").unwrap(),
            "${1}[REDACTED]",
        ),
        // AWS Access Key ID without label (standalone key starting with known prefixes)
        (
            Regex::new(r"\b(AKIA|ABIA|ACCA|ASIA)[A-Z0-9]{16}\b").unwrap(),
            "[REDACTED]",
        ),
        // AWS Secret Access Key
        (
            Regex::new(r"(?i)((?:aws_secret_access_key|secret_access_key|secretaccesskey)[=:\s]+)[A-Za-z0-9/+=]{40}").unwrap(),
            "${1}[REDACTED]",
        ),
        // Generic API key patterns
        (
            Regex::new(r#"(?i)((?:api[_-]?key|apikey|auth[_-]?token|bearer|authorization)[=:\s]+)['"]?[A-Za-z0-9_\-./+=]{20,}['"]?"#).unwrap(),
            "${1}[REDACTED]",
        ),
        // Password in key=value format (unquoted) - require = or : to avoid matching words like "CREATE SECRET"
        (
            Regex::new(r#"(?i)((?:password|passwd|pwd|secret)\s*[=:]\s*)[^\s;,)}\]"']+"#).unwrap(),
            "${1}[REDACTED]",
        ),
        // Password in key=value format (single-quoted)
        (
            Regex::new(r"(?i)((?:password|passwd|pwd|secret)\s*[=:]\s*)'[^']*'").unwrap(),
            "${1}'[REDACTED]'",
        ),
        // Password in key=value format (double-quoted)
        (
            Regex::new(r#"(?i)((?:password|passwd|pwd|secret)\s*[=:]\s*)"[^"]*""#).unwrap(),
            "${1}\"[REDACTED]\"",
        ),
        // SQL-style PASSWORD 'value' (space-separated, quoted)
        (
            Regex::new(r"(?i)(PASSWORD\s+)'[^']*'").unwrap(),
            "${1}'[REDACTED]'",
        ),
    ]
});

pub fn sanitize_credentials(input: &str) -> String {
    let mut result = input.to_string();
    for (pattern, replacement) in CREDENTIAL_PATTERNS.iter() {
        result = pattern.replace_all(&result, *replacement).into_owned();
    }
    result
}

pub struct SanitizedError(pub anyhow::Error);

impl std::fmt::Display for SanitizedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", sanitize_credentials(&self.0.to_string()))
    }
}

impl std::fmt::Debug for SanitizedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", sanitize_credentials(&format!("{:?}", self.0)))
    }
}

impl From<anyhow::Error> for SanitizedError {
    fn from(err: anyhow::Error) -> Self {
        SanitizedError(err)
    }
}

impl std::error::Error for SanitizedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

pub struct SanitizingWriter<W> {
    inner: W,
}

impl<W> SanitizingWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner }
    }
}

impl<W: Write> Write for SanitizingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let original_len = buf.len();
        if let Ok(s) = std::str::from_utf8(buf) {
            let sanitized = sanitize_credentials(s);
            self.inner.write_all(sanitized.as_bytes())?;
        } else {
            self.inner.write_all(buf)?;
        }
        Ok(original_len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[derive(Clone)]
pub struct SanitizingMakeWriter;

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SanitizingMakeWriter {
    type Writer = SanitizingWriter<std::io::Stderr>;

    fn make_writer(&'a self) -> Self::Writer {
        SanitizingWriter::new(std::io::stderr())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgresql_credentials() {
        let input = r#"Unable to connect to Postgres at "postgresql://user_abc:super_secret_password@db.example.com:5432/postgres""#;
        let sanitized = sanitize_credentials(input);
        assert!(sanitized.contains("[REDACTED]"));
        assert!(!sanitized.contains("super_secret_password"));
        assert!(sanitized.contains("db.example.com"));
    }

    #[test]
    fn test_postgres_short_protocol() {
        let input = "postgres://myuser:mypassword123@localhost:5432/mydb";
        let sanitized = sanitize_credentials(input);
        assert!(sanitized.contains("[REDACTED]"));
        assert!(!sanitized.contains("mypassword123"));
    }

    #[test]
    fn test_mysql_credentials() {
        let input = "mysql://root:password123@127.0.0.1:3306/database";
        let sanitized = sanitize_credentials(input);
        assert!(sanitized.contains("[REDACTED]"));
        assert!(!sanitized.contains("password123"));
    }

    #[test]
    fn test_mongodb_credentials() {
        let input = "mongodb://admin:secretpass@cluster.mongodb.net:27017/app";
        let sanitized = sanitize_credentials(input);
        assert!(sanitized.contains("[REDACTED]"));
        assert!(!sanitized.contains("secretpass"));
    }

    #[test]
    fn test_generic_url_credentials() {
        let input = "https://username:secretpass123@api.example.com/endpoint";
        let sanitized = sanitize_credentials(input);
        assert!(sanitized.contains("[REDACTED]"));
        assert!(!sanitized.contains("secretpass123"));
        assert!(sanitized.contains("username:"));
        assert!(sanitized.contains("@api.example.com"));
    }

    #[test]
    fn test_aws_access_key() {
        let input = "aws_access_key_id=AKIAIOSFODNN7EXAMPLE";
        let sanitized = sanitize_credentials(input);
        assert!(sanitized.contains("[REDACTED]"));
        assert!(!sanitized.contains("AKIAIOSFODNN7EXAMPLE"));
    }

    #[test]
    fn test_aws_secret_key() {
        let input = "aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let sanitized = sanitize_credentials(input);
        assert!(sanitized.contains("[REDACTED]"));
        assert!(!sanitized.contains("wJalrXUtnFEMI"));
    }

    #[test]
    fn test_api_key() {
        let input = "api_key=test_key_abcdefghijklmnopqrstuvwxyz123456";
        let sanitized = sanitize_credentials(input);
        assert!(sanitized.contains("[REDACTED]"));
        assert!(!sanitized.contains("test_key_"));
    }

    #[test]
    fn test_password_key_value() {
        let input = "password=mysecretpassword123";
        let sanitized = sanitize_credentials(input);
        assert!(sanitized.contains("[REDACTED]"));
        assert!(!sanitized.contains("mysecretpassword123"));

        let sql_input = "CREATE SECRET (PASSWORD 'topsecret123')";
        let sql_sanitized = sanitize_credentials(sql_input);
        assert!(
            sql_sanitized.contains("[REDACTED]"),
            "Should contain [REDACTED]. Got: {}",
            sql_sanitized
        );
        assert!(
            !sql_sanitized.contains("topsecret123"),
            "Should not contain password. Got: {}",
            sql_sanitized
        );
        assert!(sql_sanitized.ends_with("')"), "Closing characters should be preserved. Got: {}", sql_sanitized);
    }

    #[test]
    fn test_preserves_non_sensitive_info() {
        let input = r#"IO Error: Failed to connect to "db.example.com" (1.2.3.4), port 5432 failed: FATAL: connection refused"#;
        let sanitized = sanitize_credentials(input);
        assert!(sanitized.contains("db.example.com"));
        assert!(sanitized.contains("1.2.3.4"));
        assert!(sanitized.contains("5432"));
    }

    #[test]
    fn test_real_world_error() {
        let input = r#"IO Error: Failed to get data file list from DuckLake: Unable to connect to Postgres at "postgresql://user_05dcb297_c086_42e0_aa1c_e6addf1a4d07:VerySecretPassword123!@db.wppxcloozcjeldrhnape.supabase.co:5432/postgres": connection to server at "db.wppxcloozcjeldrhnape.supabase.co" (3.17.127.76), port 5432 failed: FATAL:  remaining connection slots are reserved for roles with the SUPERUSER attribute"#;
        let sanitized = sanitize_credentials(input);
        assert!(!sanitized.contains("VerySecretPassword123!"));
        assert!(sanitized.contains("[REDACTED]"));
        assert!(sanitized.contains("db.wppxcloozcjeldrhnape.supabase.co"));
        assert!(sanitized.contains("remaining connection slots"));
    }

    #[test]
    fn test_duckdb_ducklake_error_sanitized() {
        let conn = duckdb::Connection::open_in_memory().unwrap();

        let _ = conn.execute("INSTALL ducklake", []);
        let _ = conn.execute("LOAD ducklake", []);

        let fake_password = "SuperSecretTestPassword123!";
        let attach_sql = format!(
            "ATTACH 'ducklake:postgresql://test_user:{}@nonexistent.example.com:5432/testdb' AS test_lake",
            fake_password
        );

        let result = conn.execute(&attach_sql, []);

        assert!(result.is_err(), "Expected connection to fail");

        let error = result.unwrap_err();
        let error_str = error.to_string();

        let sanitized = sanitize_credentials(&error_str);

        assert!(
            !sanitized.contains(fake_password),
            "Sanitized error should not contain password. Got: {}",
            sanitized
        );

        if sanitized.contains("nonexistent.example.com") {
            assert!(
                sanitized.contains("[REDACTED]"),
                "If connection string is in error, it should have [REDACTED]. Got: {}",
                sanitized
            );
        }
    }

    #[tokio::test]
    async fn test_http_error_response_sanitized() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use http_body_util::BodyExt;
        use std::collections::HashMap;
        use std::sync::Arc;
        use tokio::sync::Mutex;
        use tower::ServiceExt;

        use crate::app::app;
        use crate::interfaces::DbDefaults;
        use crate::state::AppState;

        let app_state = Arc::new(AppState {
            defaults: DbDefaults {
                access_mode: "automatic".to_string(),
                cache_size: 100,
                connection_pool_size: 1,
                row_limit: 1000,
                pool_timeout: 30,
                pool_idle_timeout: 0,
                pool_max_lifetime: 0,
            },
            root: "/tmp".to_string(),
            states: Mutex::new(HashMap::new()),
            running_queries: Mutex::new(HashMap::new()),
        });

        let router = app(app_state, 30, None).await.unwrap();

        let fake_password = "SuperSecretHttpTestPassword456!";
        let body = serde_json::json!({
            "type": "exec",
            "database": ":memory:test",
            "sql": format!(
                "ATTACH 'ducklake:postgresql://test_user:{}@nonexistent.example.com:5432/testdb' AS test_lake",
                fake_password
            )
        });

        let request = Request::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap()))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();

        assert_ne!(response.status(), StatusCode::OK, "Expected request to fail");

        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let response_text = String::from_utf8(body_bytes.to_vec()).unwrap();

        assert!(
            !response_text.contains(fake_password),
            "HTTP response should not contain password. Got: {}",
            response_text
        );

        if response_text.contains("nonexistent.example.com") {
            assert!(
                response_text.contains("[REDACTED]"),
                "If connection string is in response, it should have [REDACTED]. Got: {}",
                response_text
            );
        }
    }
}
