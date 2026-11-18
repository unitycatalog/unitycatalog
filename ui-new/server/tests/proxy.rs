use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt; // for `oneshot`
use uc_ui_bridge::{app, AppState, Config};
use wiremock::matchers::{header_exists, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn test_config(uc_server: String) -> Config {
    Config {
        port: 0,
        uc_server,
        auth_enabled: true,
        google_client_id: "gid".to_string(),
        okta_enabled: false,
        keycloak_enabled: true,
        allowed_origins: vec![],
    }
}

fn build_app(uc_server: String) -> axum::Router {
    app(AppState {
        http: reqwest::Client::new(),
        config: Arc::new(test_config(uc_server)),
    })
}

async fn body_json(resp: axum::response::Response<Body>) -> serde_json::Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

fn call_request(json: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/uc.v1.UnityProxyService/Call")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json.to_string()))
        .unwrap()
}

#[tokio::test]
async fn call_forwards_get_and_maps_status_ok() {
    let uc = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/permissions/catalog/main"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"privilege_assignments":[]}"#))
        .mount(&uc)
        .await;

    let app = build_app(uc.uri());
    let resp = app
        .oneshot(call_request(serde_json::json!({
            "method": "GET",
            "path": "/api/2.1/unity-catalog/permissions/catalog/main"
        })))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert_eq!(json["httpStatus"], 200);
    assert_eq!(json["ok"], true);
    assert!(json["body"]
        .as_str()
        .unwrap()
        .contains("\"privilege_assignments\""));
}

#[tokio::test]
async fn call_maps_non_2xx_to_ok_false() {
    let uc = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/permissions/catalog/missing"))
        .respond_with(ResponseTemplate::new(404).set_body_string(r#"{"error_code":"NOT_FOUND"}"#))
        .mount(&uc)
        .await;

    let app = build_app(uc.uri());
    let resp = app
        .oneshot(call_request(serde_json::json!({
            "method": "GET",
            "path": "/api/2.1/unity-catalog/permissions/catalog/missing"
        })))
        .await
        .unwrap();

    // The RPC itself succeeds; the UC status is carried in the payload.
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert_eq!(json["httpStatus"], 404);
    // Proto JSON omits fields holding their default value; clients decode the
    // absent field as false.
    assert!(json.get("ok").is_none());
}

#[tokio::test]
async fn call_forwards_cookie_auth_and_propagates_set_cookie() {
    let uc = MockServer::start().await;
    // Assert the bridge forwarded our session cookie and UC's required CSRF
    // header, and hand back a Set-Cookie.
    Mock::given(method("POST"))
        .and(path("/api/1.0/unity-control/auth/tokens"))
        .and(wiremock::matchers::header("cookie", "UC_SESSION=abc"))
        .and(wiremock::matchers::header(
            "x-requested-with",
            "XMLHttpRequest",
        ))
        .and(wiremock::matchers::header(
            "content-type",
            "application/x-www-form-urlencoded",
        ))
        .and(wiremock::matchers::body_string("grant_type=x"))
        .respond_with(
            ResponseTemplate::new(200)
                .append_header("set-cookie", "UC_SESSION=fresh; HttpOnly; Path=/")
                .append_header("set-cookie", "UC_CSRF=fresh; Path=/")
                .set_body_string(r#"{"access_token":"t"}"#),
        )
        .mount(&uc)
        .await;

    let app = build_app(uc.uri());
    let req = Request::builder()
        .method("POST")
        .uri("/uc.v1.UnityProxyService/Call")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::COOKIE, "UC_SESSION=abc")
        .header("x-requested-with", "XMLHttpRequest")
        .body(Body::from(
            serde_json::json!({
                "method": "POST",
                "path": "/api/1.0/unity-control/auth/tokens",
                "jsonBody": "grant_type=x",
                "contentType": "application/x-www-form-urlencoded"
            })
            .to_string(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // The bridge copied UC's Set-Cookie back onto the (same-origin) response.
    let set_cookies: Vec<_> = resp
        .headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .map(|value| value.to_str().unwrap())
        .collect();
    assert_eq!(set_cookies.len(), 2);
    assert!(set_cookies[0].contains("UC_SESSION=fresh"));
    assert!(set_cookies[1].contains("UC_CSRF=fresh"));
}

#[tokio::test]
async fn call_requires_path() {
    let app = build_app("http://127.0.0.1:1".to_string());
    let resp = app
        .oneshot(call_request(serde_json::json!({ "method": "GET" })))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json = body_json(resp).await;
    assert_eq!(json["code"], "invalid_argument");
}

#[tokio::test]
async fn call_rejects_an_invalid_method() {
    let resp = build_app("http://127.0.0.1:1".to_string())
        .oneshot(call_request(serde_json::json!({
            "method": "NOT VALID",
            "path": "/api/1.0/unity-control/scim2/Me"
        })))
        .await
        .unwrap();
    let json = body_json(resp).await;
    assert_eq!(json["code"], "invalid_argument");
    assert_eq!(json["message"], "method is invalid");
}

const SCIM_ME: &str = "/api/1.0/unity-control/scim2/Me";

async fn call_me(app: axum::Router) -> serde_json::Value {
    let resp = app
        .oneshot(call_request(
            serde_json::json!({"method": "GET", "path": SCIM_ME}),
        ))
        .await
        .unwrap();
    body_json(resp).await
}

#[tokio::test]
async fn call_maps_an_unreachable_server_to_unavailable() {
    let json = call_me(build_app("http://127.0.0.1:1".to_string())).await;
    assert_eq!(json["code"], "unavailable");
}

#[tokio::test]
async fn call_maps_a_stalled_body_to_deadline_exceeded() {
    // Send the response head, then stall before the body completes.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut request).await;
        tokio::io::AsyncWriteExt::write_all(
            &mut socket,
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 64\r\n\r\n{",
        )
        .await
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    });
    let app = app(AppState {
        http: reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(200))
            .build()
            .unwrap(),
        config: Arc::new(test_config(format!("http://{address}"))),
    });

    assert_eq!(call_me(app).await["code"], "deadline_exceeded");
}

#[tokio::test]
async fn call_rejects_an_oversized_response() {
    let uc = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(SCIM_ME))
        .respond_with(ResponseTemplate::new(200).set_body_string("x".repeat(4 * 1024 * 1024 + 1)))
        .mount(&uc)
        .await;

    let json = call_me(build_app(uc.uri())).await;
    assert_eq!(json["code"], "internal");
    assert_eq!(json["message"], "Unity Catalog response is too large");
}

#[tokio::test]
async fn call_ignores_the_removed_server_url_and_token_fields() {
    // Older clients sent these. The call must still go to the configured UC,
    // and a body token must not become an Authorization header.
    let uc = MockServer::start().await;
    Mock::given(header_exists("authorization"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&uc)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/1.0/unity-control/scim2/Me"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(1)
        .mount(&uc)
        .await;

    let resp = build_app(uc.uri())
        .oneshot(call_request(serde_json::json!({
            "serverUrl": "http://169.254.169.254",
            "token": "jwt",
            "method": "GET",
            "path": "/api/1.0/unity-control/scim2/Me"
        })))
        .await
        .unwrap();

    assert_eq!(body_json(resp).await["httpStatus"], 200);
}

#[tokio::test]
async fn call_does_not_add_the_csrf_header() {
    // UC's CSRF check relies on the header coming only from the browser.
    let uc = MockServer::start().await;
    Mock::given(header_exists("x-requested-with"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&uc)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/1.0/unity-control/auth/logout"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&uc)
        .await;

    let resp = build_app(uc.uri())
        .oneshot(call_request(serde_json::json!({
            "method": "POST",
            "path": "/api/1.0/unity-control/auth/logout"
        })))
        .await
        .unwrap();

    assert_eq!(body_json(resp).await["httpStatus"], 200);
}

#[tokio::test]
async fn call_sends_one_authorization_header() {
    let uc = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/1.0/unity-control/scim2/Me"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .expect(1)
        .mount(&uc)
        .await;

    let mut req = call_request(serde_json::json!({
        "method": "GET",
        "path": "/api/1.0/unity-control/scim2/Me"
    }));
    req.headers_mut()
        .insert(header::AUTHORIZATION, "Bearer jwt".parse().unwrap());
    let resp = build_app(uc.uri()).oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let received = uc.received_requests().await.unwrap();
    let values: Vec<_> = received[0]
        .headers
        .get_all("authorization")
        .iter()
        .collect();
    assert_eq!(values, ["Bearer jwt"]);
}

#[tokio::test]
async fn call_rejects_typed_domain_bypass() {
    let app = build_app("http://127.0.0.1:1".to_string());
    let resp = app
        .oneshot(call_request(serde_json::json!({
            "method": "DELETE",
            "path": "/api/2.1/unity-catalog/catalogs/main"
        })))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let json = body_json(resp).await;
    assert_eq!(json["code"], "permission_denied");
}

#[tokio::test]
async fn call_allows_only_listed_auth_scim_and_permission_reads() {
    let uc = MockServer::start().await;
    Mock::given(wiremock::matchers::any())
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&uc)
        .await;
    let app = build_app(uc.uri());
    let cases = [
        ("GET", "/api/1.0/unity-control/scim2/Me", true),
        ("get", "/api/1.0/unity-control/scim2/Me", true),
        ("POST", "/api/1.0/unity-control/auth/tokens", true),
        ("POST", "/api/1.0/unity-control/auth/logout", true),
        (
            "GET",
            "/api/2.1/unity-catalog/permissions/registered_model/main.default.model",
            true,
        ),
        ("POST", "/api/1.0/unity-control/scim2/Me", false),
        ("GET", "/api/1.0/unity-control/scim2/Me?attributes=x", false),
        ("GET", "/api/1.0/unity-control/auth/tokens", false),
        (
            "PATCH",
            "/api/2.1/unity-catalog/permissions/catalog/main",
            false,
        ),
        (
            "GET",
            "/api/2.1/unity-catalog/permissions/metastore/main",
            false,
        ),
        ("GET", "/api/2.1/unity-catalog/permissions/catalog/", false),
        (
            "GET",
            "/api/2.1/unity-catalog/permissions/catalog/main/grants",
            false,
        ),
        (
            "GET",
            "/api/2.1/unity-catalog/permissions/catalog/..",
            false,
        ),
        (
            "GET",
            "/api/2.1/unity-catalog/permissions/catalog/%2E%2E",
            false,
        ),
        (
            "GET",
            "/api/2.1/unity-catalog/permissions/catalog/main%2F..%2F..%2Fcatalogs",
            false,
        ),
    ];

    for (call_method, call_path, allowed) in cases {
        let resp = app
            .clone()
            .oneshot(call_request(serde_json::json!({
                "method": call_method,
                "path": call_path
            })))
            .await
            .unwrap();
        if allowed {
            assert_eq!(resp.status(), StatusCode::OK, "{call_method} {call_path}");
        } else {
            assert_eq!(
                resp.status(),
                StatusCode::FORBIDDEN,
                "{call_method} {call_path}"
            );
            assert_eq!(body_json(resp).await["code"], "permission_denied");
        }
    }
    let allowed = cases.iter().filter(|(_, _, allowed)| *allowed).count();
    assert_eq!(uc.received_requests().await.unwrap().len(), allowed);
}

#[tokio::test]
async fn config_reports_auth_flags() {
    let app = build_app("http://127.0.0.1:1".to_string());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert_eq!(json["authEnabled"], true);
    assert_eq!(json["googleClientId"], "gid");
    assert_eq!(json["keycloakEnabled"], true);
    assert_eq!(json["oktaEnabled"], false);
}

#[tokio::test]
async fn healthz_ok() {
    let app = build_app("http://127.0.0.1:1".to_string());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

fn cors_app() -> axum::Router {
    app(AppState {
        http: reqwest::Client::new(),
        config: Arc::new(Config {
            allowed_origins: vec!["http://localhost:5173".to_string()],
            ..test_config("http://127.0.0.1:1".to_string())
        }),
    })
}

async fn preflight(origin: &str) -> axum::response::Response<Body> {
    cors_app()
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/uc.v1.CatalogService/DeleteCatalog")
                .header(header::ORIGIN, origin)
                .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "x-requested-with")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn cors_preflight_allows_the_csrf_header() {
    let resp = preflight("http://localhost:5173").await;
    let allowed = resp
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_HEADERS)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(allowed.contains("x-requested-with"), "{allowed}");
}

#[tokio::test]
async fn cors_preflight_rejects_an_unlisted_origin() {
    let resp = preflight("https://attacker.example").await;
    assert!(resp
        .headers()
        .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
        .is_none());
}
