use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};

use crate::AppState;

const MAX_RESPONSE_SIZE: usize = 4 * 1024 * 1024;

#[derive(Debug, Default, Deserialize)]
pub struct KeyValue {
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub value: String,
}

/// Connect JSON body of uc.v1.UnityProxyService/Call. Field names follow the
/// proto3 JSON mapping (lowerCamelCase), matching what connect-web sends.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CallRequest {
    #[serde(default)]
    pub server_url: String,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub method: String,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub query: Vec<KeyValue>,
    #[serde(default)]
    pub json_body: String,
    #[serde(default)]
    pub content_type: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CallResponse {
    pub http_status: i32,
    pub body: String,
    pub ok: bool,
}

/// A Connect protocol error: the mapped HTTP status plus a `{code, message}`
/// JSON body, which connect-web decodes into a ConnectError on the client.
fn connect_error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(serde_json::json!({ "code": code, "message": message })),
    )
        .into_response()
}

/// Handles uc.v1.UnityProxyService/Call: forwards the described REST call to the
/// Unity Catalog server and returns its status + body. Auth is cookie-based, so
/// the incoming `Cookie` header is forwarded to UC and any `Set-Cookie` from UC
/// is propagated back onto this (same-origin) response — the auth realm now sits
/// on the bridge's origin, since the browser talks to the bridge, not UC.
pub async fn call_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let req: CallRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return connect_error(
                StatusCode::BAD_REQUEST,
                "invalid_argument",
                &format!("invalid request body: {e}"),
            )
        }
    };

    if req.path.is_empty() {
        return connect_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "path is required",
        );
    }
    if !req.server_url.is_empty() {
        return connect_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "serverUrl is not supported",
        );
    }

    let method = if req.method.is_empty() {
        reqwest::Method::GET
    } else {
        match reqwest::Method::from_bytes(req.method.to_uppercase().as_bytes()) {
            Ok(method) => method,
            Err(_) => {
                return connect_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_argument",
                    "method is invalid",
                )
            }
        }
    };
    if !is_allowed_call(&method, &req.path) {
        return connect_error(
            StatusCode::FORBIDDEN,
            "permission_denied",
            "proxy call is not allowed",
        );
    }
    let url = format!(
        "{}{}",
        state.config.uc_server.trim_end_matches('/'),
        req.path
    );

    let mut rb = state.http.request(method, &url);

    let query: Vec<(&str, &str)> = req
        .query
        .iter()
        .filter(|kv| !kv.key.is_empty())
        .map(|kv| (kv.key.as_str(), kv.value.as_str()))
        .collect();
    if !query.is_empty() {
        rb = rb.query(&query);
    }

    // Forward the browser's session cookie to UC (cookie-based auth).
    if let Some(cookie) = headers.get(header::COOKIE) {
        rb = rb.header(header::COOKIE, cookie);
    }
    if let Some(authorization) = headers.get(header::AUTHORIZATION) {
        rb = rb.header(header::AUTHORIZATION, authorization);
    }
    // Optional explicit bearer token (paste-token style); usually empty.
    if !req.token.is_empty() {
        rb = rb.bearer_auth(&req.token);
    }
    if !req.json_body.is_empty() {
        let content_type = if req.content_type.is_empty() {
            "application/json"
        } else {
            req.content_type.as_str()
        };
        rb = rb
            .header(header::CONTENT_TYPE, content_type)
            .body(req.json_body.clone());
    }

    let mut upstream = match rb.send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(%e, "Unity Catalog proxy request failed");
            return connect_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "Unity Catalog server is unavailable",
            );
        }
    };

    let status = upstream.status();
    let set_cookies: Vec<HeaderValue> = upstream
        .headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .cloned()
        .collect();
    let mut bytes = Vec::new();
    loop {
        match upstream.chunk().await {
            Ok(Some(chunk)) if bytes.len() + chunk.len() <= MAX_RESPONSE_SIZE => {
                bytes.extend_from_slice(&chunk);
            }
            Ok(Some(_)) => {
                return connect_error(
                    StatusCode::BAD_GATEWAY,
                    "internal",
                    "Unity Catalog response exceeded the proxy limit",
                )
            }
            Ok(None) => break,
            Err(error) => {
                tracing::warn!(%error, "failed to read Unity Catalog proxy response");
                return connect_error(
                    StatusCode::BAD_GATEWAY,
                    "internal",
                    "Invalid response from Unity Catalog server",
                );
            }
        }
    }
    let text = String::from_utf8_lossy(&bytes).into_owned();

    let payload = CallResponse {
        http_status: status.as_u16() as i32,
        ok: status.is_success(),
        body: text,
    };

    let mut response = (StatusCode::OK, Json(payload)).into_response();
    for cookie in set_cookies {
        response.headers_mut().append(header::SET_COOKIE, cookie);
    }
    response
}

fn is_allowed_call(method: &reqwest::Method, path: &str) -> bool {
    matches!(
        (method, path),
        (&reqwest::Method::GET, "/api/1.0/unity-control/scim2/Me")
            | (&reqwest::Method::POST, "/api/1.0/unity-control/auth/tokens")
            | (&reqwest::Method::POST, "/api/1.0/unity-control/auth/logout")
    ) || (method == reqwest::Method::GET && is_permissions_path(path))
}

fn is_permissions_path(path: &str) -> bool {
    const PREFIX: &str = "/api/2.1/unity-catalog/permissions/";
    const TYPES: &[&str] = &[
        "catalog",
        "schema",
        "table",
        "volume",
        "function",
        "registered_model",
    ];
    let Some((securable_type, full_name)) = path
        .strip_prefix(PREFIX)
        .and_then(|rest| rest.split_once('/'))
    else {
        return false;
    };
    let Ok(full_name) = percent_decode_str(full_name).decode_utf8() else {
        return false;
    };
    TYPES.contains(&securable_type)
        && !full_name.is_empty()
        && !full_name.contains("..")
        && full_name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_@-.".contains(character))
}
