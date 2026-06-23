//! Error types for `dropless-core`.

use thiserror::Error;

/// Errors produced by the core engine.
#[derive(Debug, Error)]
pub enum CoreError {
    /// A database / sqlx error.
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),

    /// An outbound HTTP (reqwest) error.
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    /// Signing failed (e.g. invalid secret length).
    #[error("signing error: {0}")]
    Signing(String),

    /// JSON (de)serialization error.
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    /// A requested row was not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// An invalid value was supplied (e.g. unknown delivery status).
    #[error("invalid value: {0}")]
    Invalid(String),
}

/// Convenience result alias.
pub type CoreResult<T> = Result<T, CoreError>;
