// ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
// ┃ ██████ ██████ ██████       █      █      █      █      █ █▄  ▀███ █       ┃
// ┃ ▄▄▄▄▄█ █▄▄▄▄▄ ▄▄▄▄▄█  ▀▀▀▀▀█▀▀▀▀▀ █ ▀▀▀▀▀█ ████████▌▐███ ███▄  ▀█ █ ▀▀▀▀▀ ┃
// ┃ █▀▀▀▀▀ █▀▀▀▀▀ █▀██▀▀ ▄▄▄▄▄ █ ▄▄▄▄▄█ ▄▄▄▄▄█ ████████▌▐███ █████▄   █ ▄▄▄▄▄ ┃
// ┃ █      ██████ █  ▀█▄       █ ██████      █      ███▌▐███ ███████▄ █       ┃
// ┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫
// ┃ Copyright (c) 2017, the Perspective Authors.                              ┃
// ┃ ╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌ ┃
// ┃ This file is part of the Perspective library, distributed under the terms ┃
// ┃ of the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). ┃
// ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

use prost::{DecodeError, EncodeError};
use thiserror::Error;

/// Error type for virtual server operations.
///
/// This enum represents the various errors that can occur when processing
/// requests through a [`VirtualServer`](super::VirtualServer).
#[derive(Clone, Error, Debug)]
pub enum VirtualServerError<T: std::fmt::Debug> {
    #[error("External Error: {0:?}")]
    InternalError(#[from] T),

    #[error("{0}")]
    DecodeError(DecodeError),

    #[error("{0}")]
    EncodeError(EncodeError),

    #[error("View not found '{0}'")]
    UnknownViewId(String),

    #[error("Invalid JSON'{0}'")]
    InvalidJSON(std::sync::Arc<serde_json::Error>),

    #[error("{0}")]
    Other(String),
}

/// Extension trait for extracting internal errors from [`VirtualServerError`]
/// results.
///
/// Provides a method to distinguish between internal handler errors and other
/// virtual server errors.
pub trait ResultExt<X, T> {
    fn get_internal_error(self) -> Result<X, Result<T, String>>;
}

impl<X, T: std::fmt::Debug> ResultExt<X, T> for Result<X, VirtualServerError<T>> {
    fn get_internal_error(self) -> Result<X, Result<T, String>> {
        match self {
            Ok(x) => Ok(x),
            Err(VirtualServerError::InternalError(x)) => Err(Ok(x)),
            Err(x) => Err(Err(x.to_string())),
        }
    }
}
