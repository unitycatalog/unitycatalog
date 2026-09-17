use axum::http::header;
use connectrpc::{ConnectError, ErrorCode, RequestContext};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use reqwest::{Method, StatusCode};
use serde_json::Value;

use crate::AppState;

const API_PREFIX: &str = "/api/2.1/unity-catalog";
const MAX_RESPONSE_SIZE: usize = 32 * 1024 * 1024;

/// Shared authenticated client for the Java Unity Catalog REST API.
#[derive(Clone)]
pub(crate) struct Upstream {
    state: AppState,
}

impl Upstream {
    pub(crate) fn new(state: AppState) -> Self {
        Self { state }
    }

    pub(crate) async fn get(
        &self,
        ctx: &RequestContext,
        path: &str,
        query: Vec<(&str, String)>,
    ) -> Result<Value, ConnectError> {
        self.request(ctx, Method::GET, path, query, None).await
    }

    pub(crate) async fn post(
        &self,
        ctx: &RequestContext,
        path: &str,
        body: Value,
    ) -> Result<Value, ConnectError> {
        self.request(ctx, Method::POST, path, Vec::new(), Some(body))
            .await
    }

    pub(crate) async fn patch(
        &self,
        ctx: &RequestContext,
        path: &str,
        body: Value,
    ) -> Result<Value, ConnectError> {
        self.request(ctx, Method::PATCH, path, Vec::new(), Some(body))
            .await
    }

    pub(crate) async fn delete(
        &self,
        ctx: &RequestContext,
        path: &str,
        query: Vec<(&str, String)>,
    ) -> Result<(), ConnectError> {
        self.request(ctx, Method::DELETE, path, query, None)
            .await
            .map(|_| ())
    }

    async fn request(
        &self,
        ctx: &RequestContext,
        method: Method,
        path: &str,
        query: Vec<(&str, String)>,
        body: Option<Value>,
    ) -> Result<Value, ConnectError> {
        let expects_json = method != Method::DELETE;
        let url = format!(
            "{}{}{}",
            self.state.config.uc_server.trim_end_matches('/'),
            API_PREFIX,
            path
        );
        let mut request = self.state.http.request(method, &url).query(&query);

        if let Some(cookie) = ctx
            .header(header::COOKIE)
            .and_then(|value| value.to_str().ok())
        {
            request = request.header(header::COOKIE, cookie);
        }
        if let Some(authorization) = ctx
            .header(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
        {
            request = request.header(header::AUTHORIZATION, authorization);
        }
        if let Some(remaining) = ctx.time_remaining() {
            request = request.timeout(remaining);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }

        let mut response = request.send().await.map_err(|error| {
            tracing::warn!(%error, "Unity Catalog request failed");
            if error.is_timeout() {
                ConnectError::deadline_exceeded("Unity Catalog request timed out")
            } else {
                ConnectError::unavailable("Unity Catalog server is unavailable")
            }
        })?;
        let status = response.status();
        let mut bytes = Vec::new();
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) if bytes.len() + chunk.len() <= MAX_RESPONSE_SIZE => {
                    bytes.extend_from_slice(&chunk);
                }
                Ok(Some(_)) => {
                    return Err(ConnectError::resource_exhausted(
                        "Unity Catalog response is too large",
                    ));
                }
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!(%error, "failed to read Unity Catalog response");
                    return Err(ConnectError::internal(
                        "Invalid response from Unity Catalog server",
                    ));
                }
            }
        }

        if !status.is_success() {
            return Err(map_error(status, bytes.as_slice()));
        }
        if !expects_json || bytes.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(bytes.as_slice()).map_err(|error| {
            tracing::warn!(%error, "failed to decode Unity Catalog response");
            ConnectError::internal("Invalid response from Unity Catalog server")
        })
    }
}

pub(crate) fn path_segment(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn map_error(status: StatusCode, body: &[u8]) -> ConnectError {
    let json = serde_json::from_slice::<Value>(body).unwrap_or(Value::Null);
    let upstream_code = json
        .get("error_code")
        .or_else(|| json.get("code"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let message = json
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Unity Catalog request failed");

    let code = match upstream_code {
        "ALREADY_EXISTS" => ErrorCode::AlreadyExists,
        code if code.ends_with("_ALREADY_EXISTS") => ErrorCode::AlreadyExists,
        "INVALID_ARGUMENT" => ErrorCode::InvalidArgument,
        "FAILED_PRECONDITION" => ErrorCode::FailedPrecondition,
        "ABORTED" => ErrorCode::Aborted,
        "OUT_OF_RANGE" => ErrorCode::OutOfRange,
        "UNAUTHENTICATED" => ErrorCode::Unauthenticated,
        "PERMISSION_DENIED" => ErrorCode::PermissionDenied,
        "NOT_FOUND" => ErrorCode::NotFound,
        "RESOURCE_EXHAUSTED" => ErrorCode::ResourceExhausted,
        "UNIMPLEMENTED" => ErrorCode::Unimplemented,
        "INTERNAL" => ErrorCode::Internal,
        "UNAVAILABLE" => ErrorCode::Unavailable,
        "DATA_LOSS" => ErrorCode::DataLoss,
        _ => match status {
            StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => {
                ErrorCode::InvalidArgument
            }
            StatusCode::UNAUTHORIZED => ErrorCode::Unauthenticated,
            StatusCode::FORBIDDEN => ErrorCode::PermissionDenied,
            StatusCode::NOT_FOUND => ErrorCode::NotFound,
            StatusCode::CONFLICT => ErrorCode::Aborted,
            StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => {
                ErrorCode::DeadlineExceeded
            }
            StatusCode::TOO_MANY_REQUESTS => ErrorCode::ResourceExhausted,
            StatusCode::NOT_IMPLEMENTED => ErrorCode::Unimplemented,
            StatusCode::SERVICE_UNAVAILABLE | StatusCode::BAD_GATEWAY => ErrorCode::Unavailable,
            _ => ErrorCode::Internal,
        },
    };
    ConnectError::new(code, message)
}
