use axum::{
    extract::Request,
    http::{header::AUTHORIZATION, StatusCode},
    middleware::Next,
    response::Response,
};
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub service_account_email: String,
    pub allowed_emails: HashSet<String>,
    pub require_auth: bool,
    pub auth_token: Option<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            service_account_email: String::new(),
            allowed_emails: HashSet::new(),
            require_auth: false,
            auth_token: None,
        }
    }
}

pub async fn google_auth_middleware(
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let auth_config = request
        .extensions()
        .get::<AuthConfig>()
        .cloned()
        .unwrap_or_default();

    if !auth_config.require_auth {
        return Ok(next.run(request).await);
    }

    let auth_header = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|header| header.to_str().ok());

    let token = match auth_header {
        Some(header) if header.starts_with("Bearer ") => {
            &header[7..]
        }
        _ => {
            tracing::warn!("Missing or invalid Authorization header");
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    if !validate_auth_token(token, &auth_config) {
        tracing::warn!("Invalid authentication token");
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok(next.run(request).await)
}

fn validate_auth_token(token: &str, config: &AuthConfig) -> bool {
    if let Some(expected_token) = &config.auth_token {
        token == expected_token
    } else {
        tracing::warn!("No authentication token configured");
        false
    }
}

pub fn create_auth_config(
    service_account_email: Option<String>,
    allowed_emails: Vec<String>,
    require_auth: bool,
    auth_token: Option<String>,
) -> AuthConfig {
    AuthConfig {
        service_account_email: service_account_email.unwrap_or_default(),
        allowed_emails: allowed_emails.into_iter().collect(),
        require_auth,
        auth_token,
    }
}
