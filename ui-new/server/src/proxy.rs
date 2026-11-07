use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::AppState;

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

    let server = if req.server_url.is_empty() {
        state.config.uc_server.clone()
    } else {
        req.server_url.clone()
    };
    let url = format!("{}{}", server.trim_end_matches('/'), req.path);

    let method = if req.method.is_empty() {
        reqwest::Method::GET
    } else {
        reqwest::Method::from_bytes(req.method.to_uppercase().as_bytes())
            .unwrap_or(reqwest::Method::GET)
    };

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

    let upstream = match rb.send().await {
        Ok(r) => r,
        Err(e) => {
            return connect_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                &format!("upstream request to {url} failed: {e}"),
            )
        }
    };

    let status = upstream.status();
    let set_cookies: Vec<HeaderValue> = upstream
        .headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .cloned()
        .collect();
    let text = upstream.text().await.unwrap_or_default();

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
