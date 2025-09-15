use axum::{
    extract::{Request, State},
    http::{header::AUTHORIZATION, StatusCode},
    middleware::Next,
    response::Response,
};

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub require_auth: bool,
    pub auth_token: Option<String>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            require_auth: false,
            auth_token: None,
        }
    }
}

pub async fn selective_auth_middleware(
    State(auth_config): State<AuthConfig>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {

    let path = request.uri().path();

    if !auth_config.require_auth {
        return Ok(next.run(request).await);
    }

    let public_paths = ["/", "/healthz", "/version", "/query", "/query/"];
    
    if public_paths.contains(&path) {
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
            tracing::warn!("Missing or invalid Authorization header for protected path: {}", path);
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    if !validate_auth_token(token, &auth_config) {
        tracing::warn!("Invalid authentication token for protected path: {}", path);
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
    require_auth: bool,
    auth_token: Option<String>,
) -> AuthConfig {
    AuthConfig {
        require_auth,
        auth_token,
    }
}
