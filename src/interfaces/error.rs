use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use std::fmt;

#[derive(Debug)]
pub enum AppError {
    BadRequest(anyhow::Error),
    RetriesExceeded(anyhow::Error),
    Timeout,
    Error(anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::BadRequest(error) => {
                (StatusCode::BAD_REQUEST, format!("Bad request: {error}")).into_response()
            }
            AppError::RetriesExceeded(error) => (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("Retries exceeded: {error}"),
            )
                .into_response(),
            AppError::Timeout => (StatusCode::REQUEST_TIMEOUT).into_response(),
            AppError::Error(error) => {
                tracing::error!("Error: {:?}", error);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Something went wrong: {error}"),
                )
                    .into_response()
            }
        }
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::BadRequest(err) => write!(f, "Bad request: {}", err),
            AppError::RetriesExceeded(err) => write!(f, "Retries exceeded: {}", err),
            AppError::Timeout => write!(f, "Request timed out"),
            AppError::Error(err) => write!(f, "{}", err),
        }
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        AppError::Error(err.into())
    }
}
