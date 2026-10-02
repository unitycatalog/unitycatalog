use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt; // for `oneshot`
use uc_ui_bridge::{app, AppState, Config};
use wiremock::matchers::{method, path};
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
        // A path that certainly does not exist, so the SPA fallback stays inert.
        web_dist: std::path::PathBuf::from("/nonexistent-web-dist"),
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
        .and(path("/api/2.1/unity-catalog/catalogs"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string(r#"{"catalogs":[{"name":"main"}]}"#),
        )
        .mount(&uc)
        .await;

    let app = build_app(uc.uri());
    let resp = app
        .oneshot(call_request(serde_json::json!({
            "method": "GET",
            "path": "/api/2.1/unity-catalog/catalogs"
        })))
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert_eq!(json["httpStatus"], 200);
    assert_eq!(json["ok"], true);
    assert!(json["body"].as_str().unwrap().contains("\"main\""));
}

#[tokio::test]
async fn call_maps_non_2xx_to_ok_false() {
    let uc = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/catalogs/missing"))
        .respond_with(ResponseTemplate::new(404).set_body_string(r#"{"error_code":"NOT_FOUND"}"#))
        .mount(&uc)
        .await;

    let app = build_app(uc.uri());
    let resp = app
        .oneshot(call_request(serde_json::json!({
            "method": "GET",
            "path": "/api/2.1/unity-catalog/catalogs/missing"
        })))
        .await
        .unwrap();

    // The RPC itself succeeds; the UC status is carried in the payload.
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert_eq!(json["httpStatus"], 404);
    assert_eq!(json["ok"], false);
}

#[tokio::test]
async fn call_forwards_cookie_and_propagates_set_cookie() {
    let uc = MockServer::start().await;
    // Assert the bridge forwarded our session cookie, and hand back a Set-Cookie.
    Mock::given(method("POST"))
        .and(path("/api/1.0/unity-control/auth/tokens"))
        .and(wiremock::matchers::header("cookie", "UC_SESSION=abc"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("set-cookie", "UC_SESSION=fresh; HttpOnly; Path=/")
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
    let set_cookie = resp.headers().get(header::SET_COOKIE).unwrap();
    assert!(set_cookie.to_str().unwrap().contains("UC_SESSION=fresh"));
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
