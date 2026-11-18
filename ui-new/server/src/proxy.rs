use axum::http::header;
use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};
use percent_encoding::percent_decode_str;

use crate::proto::uc::v1::{CallRequest, CallResponse, UnityProxyService};
use crate::services::UiRpc;

// Auth, SCIM, and permission replies are small, so the proxy reads less than
// the typed services do.
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

        let mut upstream = self.upstream.builder(method, request.path);
        let query: Vec<_> = request
            .query
            .iter()
            .filter(|item| !item.key.is_empty())
            .map(|item| (item.key, item.value))
            .collect();
        if !query.is_empty() {
            upstream = upstream.query(&query);
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

        let upstream = self
            .upstream
            .send(&ctx, upstream, MAX_RESPONSE_SIZE)
            .await?;
        let mut response = Response::new(CallResponse {
            http_status: upstream.status.as_u16().into(),
            body: String::from_utf8_lossy(&upstream.bytes).into_owned(),
            ok: upstream.status.is_success(),
            ..Default::default()
        });
        for cookie in upstream.headers.get_all(header::SET_COOKIE) {
            response.headers.append(header::SET_COOKIE, cookie.clone());
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
