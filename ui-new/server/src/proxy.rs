use axum::http::{header, HeaderValue};
use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};
use percent_encoding::percent_decode_str;

use crate::proto::uc::v1::{CallRequest, CallResponse, UnityProxyService};
use crate::services::UiRpc;
use crate::FORWARDED_HEADERS;

const MAX_RESPONSE_SIZE: usize = 4 * 1024 * 1024;

#[protovalidate_buffa::connect_impl]
impl UnityProxyService for UiRpc {
    async fn call(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CallRequest>,
    ) -> ServiceResult<CallResponse> {
        if request.path.is_empty() {
            return Err(ConnectError::invalid_argument("path is required"));
        }

        let method = if request.method.is_empty() {
            reqwest::Method::GET
        } else {
            reqwest::Method::from_bytes(request.method.to_uppercase().as_bytes())
                .map_err(|_| ConnectError::invalid_argument("method is invalid"))?
        };
        if !is_allowed_call(&method, request.path) {
            return Err(ConnectError::permission_denied("proxy call is not allowed"));
        }

        let url = format!(
            "{}{}",
            self.state.config.uc_server.trim_end_matches('/'),
            request.path
        );
        let mut upstream = self.state.http.request(method, &url);
        let query: Vec<_> = request
            .query
            .iter()
            .filter(|item| !item.key.is_empty())
            .map(|item| (item.key, item.value))
            .collect();
        if !query.is_empty() {
            upstream = upstream.query(&query);
        }
        for name in FORWARDED_HEADERS {
            if let Some(value) = ctx.header(&name) {
                upstream = upstream.header(name, value);
            }
        }
        if let Some(remaining) = ctx.time_remaining() {
            upstream = upstream.timeout(remaining);
        }
        if !request.json_body.is_empty() {
            let content_type = if request.content_type.is_empty() {
                "application/json"
            } else {
                request.content_type
            };
            upstream = upstream
                .header(header::CONTENT_TYPE, content_type)
                .body(request.json_body.to_string());
        }

        let mut upstream = upstream.send().await.map_err(|error| {
            tracing::warn!(%error, "Unity Catalog proxy request failed");
            if error.is_timeout() {
                ConnectError::deadline_exceeded("Unity Catalog request timed out")
            } else {
                ConnectError::unavailable("Unity Catalog server is unavailable")
            }
        })?;
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
                    return Err(ConnectError::resource_exhausted(
                        "Unity Catalog response exceeded the proxy limit",
                    ));
                }
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!(%error, "failed to read Unity Catalog proxy response");
                    return Err(ConnectError::internal(
                        "Invalid response from Unity Catalog server",
                    ));
                }
            }
        }

        let mut response = Response::new(CallResponse {
            http_status: status.as_u16().into(),
            body: String::from_utf8_lossy(&bytes).into_owned(),
            ok: status.is_success(),
            ..Default::default()
        });
        for cookie in set_cookies {
            response.headers.append(header::SET_COOKIE, cookie);
        }
        Ok(response)
    }
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
