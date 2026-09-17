use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;
use uc_ui_bridge::{app, AppState, Config};
use wiremock::matchers::{
    body_partial_json, header as header_match, method, path, query_param, query_param_is_missing,
};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn build_app(uc_server: String) -> axum::Router {
    app(AppState {
        http: reqwest::Client::new(),
        config: Arc::new(Config {
            port: 0,
            uc_server,
            auth_enabled: true,
            google_client_id: String::new(),
            okta_enabled: false,
            keycloak_enabled: false,
            allowed_origins: vec![],
            web_dist: std::path::PathBuf::from("/nonexistent-web-dist"),
        }),
    })
}

fn rpc_request(service: &str, method: &str, body: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!("/uc.v1.{service}/{method}"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("connect-protocol-version", "1")
        .header(header::AUTHORIZATION, "Bearer user-token")
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn body_json(response: axum::response::Response<Body>) -> serde_json::Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn list_catalogs_forwards_auth_and_pagination() {
    let uc = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/catalogs"))
        .and(query_param("max_results", "10"))
        .and(query_param("page_token", "next"))
        .and(header_match("authorization", "Bearer user-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "catalogs": [{"name": "main", "owner": "owner@example.com"}],
            "next_page_token": "later"
        })))
        .expect(1)
        .mount(&uc)
        .await;

    let response = build_app(uc.uri())
        .oneshot(rpc_request(
            "CatalogService",
            "ListCatalogs",
            serde_json::json!({"page": {"maxResults": 10, "pageToken": "next"}}),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["catalogs"][0]["name"], "main");
    assert_eq!(json["catalogs"][0]["audit"]["owner"], "owner@example.com");
    assert_eq!(json["page"]["nextPageToken"], "later");
}

#[tokio::test]
async fn validation_rejects_invalid_catalog_name_before_upstream() {
    let uc = MockServer::start().await;
    let response = build_app(uc.uri())
        .oneshot(rpc_request(
            "CatalogService",
            "GetCatalog",
            serde_json::json!({"catalog": {"name": "invalid.name"}}),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_eq!(json["code"], "invalid_argument");
}

#[tokio::test]
async fn validation_rejects_external_volume_without_storage() {
    let uc = MockServer::start().await;
    let response = build_app(uc.uri())
        .oneshot(rpc_request(
            "VolumeService",
            "CreateVolume",
            serde_json::json!({
                "volume": {"catalogName": "main", "schemaName": "default", "name": "raw"},
                "volumeType": "VOLUME_TYPE_EXTERNAL"
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_eq!(json["code"], "invalid_argument");
}

#[tokio::test]
async fn create_view_rejects_invalid_metric_yaml() {
    let uc = MockServer::start().await;
    let response = build_app(uc.uri())
        .oneshot(rpc_request(
            "ViewService",
            "CreateView",
            serde_json::json!({
                "view": {"catalogName": "main", "schemaName": "default", "name": "metrics"},
                "columns": [{
                    "name": "total",
                    "typeName": "COLUMN_TYPE_NAME_LONG"
                }],
                "viewDefinition": "dimensions: [",
                "dependencies": [{
                    "tableFullName": "main.default.events"
                }]
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_eq!(json["code"], "invalid_argument");
    assert_eq!(json["message"], "Invalid metric view YAML definition");
}

#[tokio::test]
async fn create_view_requires_columns_to_match_yaml_fields_exactly() {
    let uc = MockServer::start().await;
    let response = build_app(uc.uri())
        .oneshot(rpc_request(
            "ViewService",
            "CreateView",
            serde_json::json!({
                "view": {"catalogName": "main", "schemaName": "default", "name": "metrics"},
                "columns": [{
                    "name": "event_day",
                    "typeName": "COLUMN_TYPE_NAME_DATE"
                }],
                "viewDefinition": "dimensions:\n  - name: event_day\n    expr: event_day\nmeasures:\n  - name: total\n    expr: count(*)",
                "dependencies": [{
                    "tableFullName": "main.default.events"
                }]
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        body_json(response).await["message"],
        "Metric view columns are missing YAML fields: total"
    );
}

#[tokio::test]
async fn create_view_rejects_duplicate_columns() {
    let uc = MockServer::start().await;
    let response = build_app(uc.uri())
        .oneshot(rpc_request(
            "ViewService",
            "CreateView",
            serde_json::json!({
                "view": {"catalogName": "main", "schemaName": "default", "name": "metrics"},
                "columns": [
                    {"name": "event_day", "typeName": "COLUMN_TYPE_NAME_DATE"},
                    {"name": "event_day", "typeName": "COLUMN_TYPE_NAME_DATE"}
                ],
                "viewDefinition": "dimensions:\n  - name: event_day\n    expr: event_day",
                "dependencies": [{
                    "tableFullName": "main.default.events"
                }]
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        body_json(response).await["message"],
        "Metric view column names must be unique: event_day"
    );
}

#[tokio::test]
async fn create_table_derives_uc_column_metadata() {
    let uc = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.1/unity-catalog/tables"))
        .and(body_partial_json(serde_json::json!({
            "name": "events",
            "catalog_name": "main",
            "schema_name": "default",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "storage_location": "s3://bucket/events",
            "columns": [{
                "name": "id",
                "type_name": "STRING",
                "type_text": "string",
                "nullable": false,
                "position": 0
            }]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "name": "events",
            "catalog_name": "main",
            "schema_name": "default",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "columns": []
        })))
        .expect(1)
        .mount(&uc)
        .await;

    let response = build_app(uc.uri())
        .oneshot(rpc_request(
            "TableService",
            "CreateTable",
            serde_json::json!({
                "table": {"catalogName": "main", "schemaName": "default", "name": "events"},
                "dataSourceFormat": "DATA_SOURCE_FORMAT_DELTA",
                "columns": [{
                    "name": "id",
                    "typeName": "COLUMN_TYPE_NAME_STRING",
                    "nullable": false
                }],
                "storageLocation": {"value": "s3://bucket/events"}
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["table"]["name"], "events");
    assert_eq!(json["table"]["fullName"], "main.default.events");
}

#[tokio::test]
async fn validation_rejects_case_insensitive_duplicate_table_columns() {
    let uc = MockServer::start().await;
    let response = build_app(uc.uri())
        .oneshot(rpc_request(
            "TableService",
            "CreateTable",
            serde_json::json!({
                "table": {"catalogName": "main", "schemaName": "default", "name": "events"},
                "dataSourceFormat": "DATA_SOURCE_FORMAT_DELTA",
                "columns": [
                    {"name": "id", "typeName": "COLUMN_TYPE_NAME_STRING"},
                    {"name": "ID", "typeName": "COLUMN_TYPE_NAME_STRING"}
                ],
                "storageLocation": {"value": "s3://bucket/events"}
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_eq!(json["code"], "invalid_argument");
}

#[tokio::test]
async fn create_function_uses_uc_request_envelope() {
    let uc = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.1/unity-catalog/functions"))
        .and(body_partial_json(serde_json::json!({
            "function_info": {
                "name": "answer",
                "catalog_name": "main",
                "schema_name": "default",
                "data_type": "LONG",
                "full_data_type": "bigint",
                "routine_body": "SQL",
                "routine_definition": "RETURN 42"
            }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "name": "answer",
            "catalog_name": "main",
            "schema_name": "default",
            "data_type": "LONG",
            "full_data_type": "bigint",
            "routine_body": "SQL",
            "routine_definition": "RETURN 42"
        })))
        .expect(1)
        .mount(&uc)
        .await;

    let response = build_app(uc.uri())
        .oneshot(rpc_request(
            "FunctionService",
            "CreateFunction",
            serde_json::json!({
                "function": {"catalogName": "main", "schemaName": "default", "name": "answer"},
                "returnType": "COLUMN_TYPE_NAME_LONG",
                "routineDefinition": "RETURN 42"
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_json(response).await["function"]["name"], "answer");
}

#[tokio::test]
async fn list_views_advances_past_table_only_pages() {
    let uc = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/tables"))
        .and(query_param("catalog_name", "main"))
        .and(query_param("schema_name", "default"))
        .and(query_param_is_missing("page_token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tables": [{"name": "events", "table_type": "MANAGED"}],
            "next_page_token": "next"
        })))
        .expect(1)
        .mount(&uc)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/tables"))
        .and(query_param("page_token", "next"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tables": [{
                "name": "metrics",
                "catalog_name": "main",
                "schema_name": "default",
                "table_type": "METRIC_VIEW"
            }]
        })))
        .expect(1)
        .mount(&uc)
        .await;

    let response = build_app(uc.uri())
        .oneshot(rpc_request(
            "ViewService",
            "ListViews",
            serde_json::json!({
                "schema": {"catalogName": "main", "name": "default"}
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["views"][0]["fullName"], "main.default.metrics");
}

#[tokio::test]
async fn delete_services_refuse_the_other_table_type() {
    let uc = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/api/2.1/unity-catalog/tables/main%2Edefault%2Emetrics",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "name": "metrics",
            "table_type": "METRIC_VIEW"
        })))
        .expect(1)
        .mount(&uc)
        .await;
    Mock::given(method("GET"))
        .and(path(
            "/api/2.1/unity-catalog/tables/main%2Edefault%2Eevents",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "name": "events",
            "table_type": "MANAGED"
        })))
        .expect(1)
        .mount(&uc)
        .await;
    let app = build_app(uc.uri());

    let table_response = app
        .clone()
        .oneshot(rpc_request(
            "TableService",
            "DeleteTable",
            serde_json::json!({
                "table": {"catalogName": "main", "schemaName": "default", "name": "metrics"}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(table_response.status(), StatusCode::NOT_FOUND);

    let view_response = app
        .oneshot(rpc_request(
            "ViewService",
            "DeleteView",
            serde_json::json!({
                "view": {"catalogName": "main", "schemaName": "default", "name": "events"}
            }),
        ))
        .await
        .unwrap();
    assert_eq!(view_response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn delete_accepts_non_json_success_body() {
    let uc = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/api/2.1/unity-catalog/volumes/main%2Edefault%2Edata"))
        .respond_with(ResponseTemplate::new(200).set_body_string("OK"))
        .expect(1)
        .mount(&uc)
        .await;

    let response = build_app(uc.uri())
        .oneshot(rpc_request(
            "VolumeService",
            "DeleteVolume",
            serde_json::json!({
                "volume": {"catalogName": "main", "schemaName": "default", "name": "data"}
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_json(response).await, serde_json::json!({}));
}

#[tokio::test]
async fn upstream_not_found_maps_to_connect_not_found() {
    let uc = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/catalogs/missing"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "error_code": "CATALOG_DOES_NOT_EXIST",
            "message": "Catalog not found: missing"
        })))
        .mount(&uc)
        .await;

    let response = build_app(uc.uri())
        .oneshot(rpc_request(
            "CatalogService",
            "GetCatalog",
            serde_json::json!({"catalog": {"name": "missing"}}),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let json = body_json(response).await;
    assert_eq!(json["code"], "not_found");
    assert_eq!(json["message"], "Catalog not found: missing");
}

#[tokio::test]
async fn upstream_exact_already_exists_code_is_preserved() {
    let uc = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.1/unity-catalog/models"))
        .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
            "error_code": "ALREADY_EXISTS",
            "message": "Model already exists"
        })))
        .mount(&uc)
        .await;

    let response = build_app(uc.uri())
        .oneshot(rpc_request(
            "ModelService",
            "CreateRegisteredModel",
            serde_json::json!({
                "model": {"catalogName": "main", "schemaName": "default", "name": "model"}
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(response).await["code"], "already_exists");
}

#[tokio::test]
async fn upstream_gateway_timeout_maps_to_deadline_exceeded() {
    let uc = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/catalogs/slow"))
        .respond_with(
            ResponseTemplate::new(504)
                .set_body_json(serde_json::json!({"message": "Upstream timed out"})),
        )
        .mount(&uc)
        .await;

    let response = build_app(uc.uri())
        .oneshot(rpc_request(
            "CatalogService",
            "GetCatalog",
            serde_json::json!({"catalog": {"name": "slow"}}),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    let json = body_json(response).await;
    assert_eq!(json["code"], "deadline_exceeded");
}
