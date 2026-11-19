use connectrpc::{ConnectError, ErrorCode, RequestContext};
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use reqwest::header::HeaderMap;
use reqwest::{Method, RequestBuilder, StatusCode};
use serde_json::Value;

use crate::{AppState, FORWARDED_HEADERS};

const API_PREFIX: &str = "/api/2.1/unity-catalog";
const MAX_RESPONSE_SIZE: usize = 32 * 1024 * 1024;

/// A Unity Catalog response, read in full within a size limit.
pub(crate) struct RawResponse {
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
    pub(crate) bytes: Vec<u8>,
}

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

    /// Starts a request to `path`, relative to the configured UC server.
    pub(crate) fn builder(&self, method: Method, path: &str) -> RequestBuilder {
        let url = format!(
            "{}{path}",
            self.state.config.uc_server.trim_end_matches('/')
        );
        self.state.http.request(method, url)
    }

    /// Sends `request` with the caller's forwarded headers and deadline, then
    /// reads at most `max_size` bytes of the response.
    pub(crate) async fn send(
        &self,
        ctx: &RequestContext,
        mut request: RequestBuilder,
        max_size: usize,
    ) -> Result<RawResponse, ConnectError> {
        for name in FORWARDED_HEADERS {
            if let Some(value) = ctx.header(&name) {
                request = request.header(name, value);
            }
        }
        if let Some(remaining) = ctx.time_remaining() {
            request = request.timeout(remaining);
        }

        let mut response = request.send().await.map_err(|error| {
            tracing::warn!(%error, "Unity Catalog request failed");
            transport_error(&error)
        })?;
        let status = response.status();
        let headers = std::mem::take(response.headers_mut());
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            tracing::warn!(%error, "failed to read Unity Catalog response");
            transport_error(&error)
        })? {
            if bytes.len() + chunk.len() > max_size {
                return Err(ConnectError::internal(
                    "Unity Catalog response is too large",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(RawResponse {
            status,
            headers,
            bytes,
        })
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
        let mut request = self
            .builder(method, &format!("{API_PREFIX}{path}"))
            .query(&query);
        if let Some(body) = body {
            request = request.json(&body);
        }

        let response = self.send(ctx, request, MAX_RESPONSE_SIZE).await?;
        if !response.status.is_success() {
            return Err(map_error(response.status, &response.bytes));
        }
        if !expects_json || response.bytes.is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_slice(&response.bytes).map_err(|error| {
            tracing::warn!(%error, "failed to decode Unity Catalog response");
            ConnectError::internal("Invalid response from Unity Catalog server")
        })
    }
}

fn transport_error(error: &reqwest::Error) -> ConnectError {
    if error.is_timeout() {
        ConnectError::deadline_exceeded("Unity Catalog request timed out")
    } else {
        ConnectError::unavailable("Unity Catalog server is unavailable")
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
