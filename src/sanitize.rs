use regex::Regex;
use std::io::Write;
use std::sync::LazyLock;

/// Regex patterns for credential sanitization
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
            Regex::new(r"(?i)((?:aws_access_key_id|access_key_id|accesskeyid)[=:\s]+)[A-Z0-9]{20}").unwrap(),
            "${1}[REDACTED]",
        ),
        // AWS Secret Access Key
        (
            Regex::new(r"(?i)((?:aws_secret_access_key|secret_access_key|secretaccesskey)[=:\s]+)[A-Za-z0-9/+=]{40}").unwrap(),
            "${1}[REDACTED]",
        ),
        // Generic API key patterns
        (
            Regex::new(r#"(?i)((?:api[_-]?key|apikey|auth[_-]?token|bearer|authorization)[=:\s]+)['"]?[A-Za-z0-9_\-.\/+=]{20,}['"]?"#).unwrap(),
            "${1}[REDACTED]",
        ),
        // Password in key=value format
        (
            Regex::new(r"(?i)((?:password|passwd|pwd|secret)[=:\s]+)[^\s;,]+").unwrap(),
            "${1}[REDACTED]",
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

impl<E: Into<anyhow::Error>> From<E> for SanitizedError {
    fn from(err: E) -> Self {
        SanitizedError(err.into())
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
        let input = "https://username:password@api.example.com/endpoint";
        let sanitized = sanitize_credentials(input);
        assert!(sanitized.contains("[REDACTED]"));
        assert!(!sanitized.contains("password"));
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
    }

    #[test]
    fn test_preserves_non_sensitive_info() {
        let input = r#"IO Error: Failed to connect to "db.example.com" (1.2.3.4), port 5432 failed: FATAL: connection refused"#;
        let sanitized = sanitize_credentials(input);
        // Should preserve the error message structure
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
}
