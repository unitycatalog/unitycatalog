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
        .and(header_match("cookie", "UC_TOKEN=session"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "catalogs": [{"name": "main", "owner": "owner@example.com"}],
            "next_page_token": "later"
        })))
        .expect(1)
        .mount(&uc)
        .await;

    let mut request = rpc_request(
        "CatalogService",
        "ListCatalogs",
        serde_json::json!({"page": {"maxResults": 10, "pageToken": "next"}}),
    );
    request
        .headers_mut()
        .insert(header::COOKIE, "UC_TOKEN=session".parse().unwrap());
    let response = build_app(uc.uri()).oneshot(request).await.unwrap();

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
    let message = json["message"].as_str().unwrap();
    assert!(
        message.starts_with("Invalid metric view YAML definition: ")
            && message.contains(" at line "),
        "{message}"
    );
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
async fn list_views_returns_filtered_empty_page_with_next_token() {
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
    assert!(json.get("views").is_none());
    assert_eq!(json["page"]["nextPageToken"], "next");
}

#[tokio::test]
async fn list_tables_omits_metric_views() {
    let uc = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/tables"))
        .and(query_param("catalog_name", "main"))
        .and(query_param("schema_name", "default"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "tables": [
                {
                    "name": "events",
                    "catalog_name": "main",
                    "schema_name": "default",
                    "table_type": "MANAGED"
                },
                {
                    "name": "metrics",
                    "catalog_name": "main",
                    "schema_name": "default",
                    "table_type": "METRIC_VIEW"
                }
            ]
        })))
        .expect(1)
        .mount(&uc)
        .await;

    let response = build_app(uc.uri())
        .oneshot(rpc_request(
            "TableService",
            "ListTables",
            serde_json::json!({
                "schema": {"catalogName": "main", "name": "default"}
            }),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let names: Vec<_> = json["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|table| table["fullName"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["main.default.events"]);
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
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
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
    assert_eq!(
        body_json(table_response).await["message"],
        "Table not found: main.default.metrics"
    );

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
    assert_eq!(
        body_json(view_response).await["message"],
        "Metric view not found: main.default.events"
    );
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

#[tokio::test]
async fn upstream_errors_map_to_connect_codes() {
    let uc = MockServer::start().await;
    let cases = [
        (
            409,
            r#"{"error_code":"CATALOG_ALREADY_EXISTS"}"#,
            "already_exists",
        ),
        (500, r#"{"code":"PERMISSION_DENIED"}"#, "permission_denied"),
        (400, "{}", "invalid_argument"),
        (401, "{}", "unauthenticated"),
        (403, "{}", "permission_denied"),
        (409, "{}", "aborted"),
        (429, "{}", "resource_exhausted"),
        (503, "{}", "unavailable"),
        (500, "not json", "internal"),
    ];
    for (index, (status, body, _)) in cases.iter().enumerate() {
        Mock::given(method("GET"))
            .and(path(format!("/api/2.1/unity-catalog/catalogs/case{index}")))
            .respond_with(ResponseTemplate::new(*status).set_body_string(*body))
            .mount(&uc)
            .await;
    }
    let app = build_app(uc.uri());

    for (index, (status, body, code)) in cases.iter().enumerate() {
        let response = app
            .clone()
            .oneshot(rpc_request(
                "CatalogService",
                "GetCatalog",
                serde_json::json!({"catalog": {"name": format!("case{index}")}}),
            ))
            .await
            .unwrap();
        let json = body_json(response).await;
        assert_eq!(json["code"], *code, "UC {status} {body}");
        if !body.starts_with('{') {
            assert_eq!(json["message"], "Unity Catalog request failed");
        }
    }
}

#[tokio::test]
async fn unreachable_upstream_maps_to_unavailable() {
    let response = build_app("http://127.0.0.1:1".to_string())
        .oneshot(rpc_request(
            "CatalogService",
            "GetCatalog",
            serde_json::json!({"catalog": {"name": "main"}}),
        ))
        .await
        .unwrap();

    let json = body_json(response).await;
    assert_eq!(json["code"], "unavailable");
    assert_eq!(json["message"], "Unity Catalog server is unavailable");
}

#[tokio::test]
async fn upstream_body_timeout_maps_to_deadline_exceeded() {
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
        config: Arc::new(Config {
            port: 0,
            uc_server: format!("http://{address}"),
            auth_enabled: true,
            google_client_id: String::new(),
            okta_enabled: false,
            keycloak_enabled: false,
            allowed_origins: vec![],
            web_dist: std::path::PathBuf::from("/nonexistent-web-dist"),
        }),
    });

    let response = app
        .oneshot(rpc_request(
            "CatalogService",
            "GetCatalog",
            serde_json::json!({"catalog": {"name": "main"}}),
        ))
        .await
        .unwrap();

    assert_eq!(body_json(response).await["code"], "deadline_exceeded");
}

struct UcCall {
    service: &'static str,
    rpc: &'static str,
    request: serde_json::Value,
    method: &'static str,
    path: &'static str,
    query: &'static [(&'static str, &'static str)],
    body: Option<serde_json::Value>,
    response: serde_json::Value,
}

#[tokio::test]
async fn typed_rpcs_send_the_expected_uc_requests() {
    use serde_json::json;
    let schema = json!({"catalogName": "main", "name": "default"});
    let object = |name: &str| json!({"catalogName": "main", "schemaName": "default", "name": name});
    let version = json!({"model": object("m"), "version": "3"});
    let catalog_info = json!({"name": "main"});
    let schema_info = json!({"name": "default", "catalog_name": "main"});
    let uc_object =
        |name: &str| json!({"name": name, "catalog_name": "main", "schema_name": "default"});
    let version_info = json!({
        "catalog_name": "main",
        "schema_name": "default",
        "model_name": "m",
        "version": 3
    });
    let cases = [
        UcCall {
            service: "CatalogService",
            rpc: "CreateCatalog",
            request: json!({"name": "main", "comment": "c"}),
            method: "POST",
            path: "/catalogs",
            query: &[],
            body: Some(json!({"name": "main", "comment": "c"})),
            response: catalog_info.clone(),
        },
        UcCall {
            service: "CatalogService",
            rpc: "UpdateCatalog",
            request: json!({"catalog": {"name": "main"}, "newName": "renamed"}),
            method: "PATCH",
            path: "/catalogs/main",
            query: &[],
            body: Some(json!({"new_name": "renamed"})),
            response: catalog_info.clone(),
        },
        UcCall {
            service: "CatalogService",
            rpc: "DeleteCatalog",
            request: json!({"catalog": {"name": "main"}, "force": true}),
            method: "DELETE",
            path: "/catalogs/main",
            query: &[("force", "true")],
            body: None,
            response: json!({}),
        },
        UcCall {
            service: "SchemaService",
            rpc: "ListSchemas",
            request: json!({"catalog": {"name": "main"}, "page": {"maxResults": 5}}),
            method: "GET",
            path: "/schemas",
            query: &[("catalog_name", "main"), ("max_results", "5")],
            body: None,
            response: json!({"schemas": [schema_info.clone()]}),
        },
        UcCall {
            service: "SchemaService",
            rpc: "GetSchema",
            request: json!({"schema": schema.clone()}),
            method: "GET",
            path: "/schemas/main%2Edefault",
            query: &[],
            body: None,
            response: schema_info.clone(),
        },
        UcCall {
            service: "SchemaService",
            rpc: "CreateSchema",
            request: json!({"schema": schema.clone(), "comment": "c"}),
            method: "POST",
            path: "/schemas",
            query: &[],
            body: Some(json!({"name": "default", "catalog_name": "main", "comment": "c"})),
            response: schema_info.clone(),
        },
        UcCall {
            service: "SchemaService",
            rpc: "UpdateSchema",
            request: json!({"schema": schema.clone(), "newName": "renamed"}),
            method: "PATCH",
            path: "/schemas/main%2Edefault",
            query: &[],
            body: Some(json!({"new_name": "renamed"})),
            response: schema_info.clone(),
        },
        UcCall {
            service: "SchemaService",
            rpc: "DeleteSchema",
            request: json!({"schema": schema.clone(), "force": true}),
            method: "DELETE",
            path: "/schemas/main%2Edefault",
            query: &[("force", "true")],
            body: None,
            response: json!({}),
        },
        UcCall {
            service: "VolumeService",
            rpc: "ListVolumes",
            request: json!({"schema": schema.clone()}),
            method: "GET",
            path: "/volumes",
            query: &[("catalog_name", "main"), ("schema_name", "default")],
            body: None,
            response: json!({"volumes": [uc_object("raw")]}),
        },
        UcCall {
            service: "VolumeService",
            rpc: "GetVolume",
            request: json!({"volume": object("raw")}),
            method: "GET",
            path: "/volumes/main%2Edefault%2Eraw",
            query: &[],
            body: None,
            response: uc_object("raw"),
        },
        UcCall {
            service: "VolumeService",
            rpc: "CreateVolume",
            request: json!({"volume": object("raw"), "volumeType": "VOLUME_TYPE_MANAGED"}),
            method: "POST",
            path: "/volumes",
            query: &[],
            body: Some(json!({
                "name": "raw",
                "catalog_name": "main",
                "schema_name": "default",
                "volume_type": "MANAGED"
            })),
            response: uc_object("raw"),
        },
        UcCall {
            service: "VolumeService",
            rpc: "UpdateVolume",
            request: json!({"volume": object("raw"), "comment": ""}),
            method: "PATCH",
            path: "/volumes/main%2Edefault%2Eraw",
            query: &[],
            body: Some(json!({"comment": ""})),
            response: uc_object("raw"),
        },
        UcCall {
            service: "FunctionService",
            rpc: "ListFunctions",
            request: json!({"schema": schema.clone()}),
            method: "GET",
            path: "/functions",
            query: &[("catalog_name", "main"), ("schema_name", "default")],
            body: None,
            response: json!({"functions": [uc_object("f")]}),
        },
        UcCall {
            service: "FunctionService",
            rpc: "GetFunction",
            request: json!({"function": object("f")}),
            method: "GET",
            path: "/functions/main%2Edefault%2Ef",
            query: &[],
            body: None,
            response: uc_object("f"),
        },
        UcCall {
            service: "FunctionService",
            rpc: "DeleteFunction",
            request: json!({"function": object("f")}),
            method: "DELETE",
            path: "/functions/main%2Edefault%2Ef",
            query: &[],
            body: None,
            response: json!({}),
        },
        UcCall {
            service: "ModelService",
            rpc: "ListRegisteredModels",
            request: json!({"schema": schema.clone()}),
            method: "GET",
            path: "/models",
            query: &[("catalog_name", "main"), ("schema_name", "default")],
            body: None,
            response: json!({"registered_models": [uc_object("m")]}),
        },
        UcCall {
            service: "ModelService",
            rpc: "GetRegisteredModel",
            request: json!({"model": object("m")}),
            method: "GET",
            path: "/models/main%2Edefault%2Em",
            query: &[],
            body: None,
            response: uc_object("m"),
        },
        UcCall {
            service: "ModelService",
            rpc: "UpdateRegisteredModel",
            request: json!({"model": object("m"), "comment": "c"}),
            method: "PATCH",
            path: "/models/main%2Edefault%2Em",
            query: &[],
            body: Some(json!({"comment": "c"})),
            response: uc_object("m"),
        },
        UcCall {
            service: "ModelService",
            rpc: "DeleteRegisteredModel",
            request: json!({"model": object("m"), "force": true}),
            method: "DELETE",
            path: "/models/main%2Edefault%2Em",
            query: &[("force", "true")],
            body: None,
            response: json!({}),
        },
        UcCall {
            service: "ModelService",
            rpc: "ListModelVersions",
            request: json!({"model": object("m")}),
            method: "GET",
            path: "/models/main%2Edefault%2Em/versions",
            query: &[],
            body: None,
            response: json!({"model_versions": [version_info.clone()]}),
        },
        UcCall {
            service: "ModelService",
            rpc: "GetModelVersion",
            request: json!({"modelVersion": version.clone()}),
            method: "GET",
            path: "/models/main%2Edefault%2Em/versions/3",
            query: &[],
            body: None,
            response: version_info.clone(),
        },
        UcCall {
            service: "ModelService",
            rpc: "CreateModelVersion",
            request: json!({"model": object("m"), "source": "s3://bucket/m", "runId": "r1"}),
            method: "POST",
            path: "/models/versions",
            query: &[],
            body: Some(json!({
                "model_name": "m",
                "catalog_name": "main",
                "schema_name": "default",
                "source": "s3://bucket/m",
                "run_id": "r1"
            })),
            response: version_info.clone(),
        },
        UcCall {
            service: "ModelService",
            rpc: "UpdateModelVersion",
            request: json!({"modelVersion": version.clone(), "comment": "c"}),
            method: "PATCH",
            path: "/models/main%2Edefault%2Em/versions/3",
            query: &[],
            body: Some(json!({"comment": "c"})),
            response: version_info.clone(),
        },
        UcCall {
            service: "ModelService",
            rpc: "FinalizeModelVersion",
            request: json!({"modelVersion": version.clone()}),
            method: "PATCH",
            path: "/models/main%2Edefault%2Em/versions/3/finalize",
            query: &[],
            body: Some(json!({"full_name": "main.default.m", "version": 3})),
            response: version_info.clone(),
        },
        UcCall {
            service: "ModelService",
            rpc: "DeleteModelVersion",
            request: json!({"modelVersion": version.clone()}),
            method: "DELETE",
            path: "/models/main%2Edefault%2Em/versions/3",
            query: &[],
            body: None,
            response: json!({}),
        },
    ];

    for case in cases {
        let uc = MockServer::start().await;
        let mut mock = Mock::given(method(case.method))
            .and(path(format!("/api/2.1/unity-catalog{}", case.path)));
        for (key, value) in case.query {
            mock = mock.and(query_param(*key, *value));
        }
        if let Some(body) = case.body {
            mock = mock.and(body_partial_json(body));
        }
        mock.respond_with(ResponseTemplate::new(200).set_body_json(case.response))
            .expect(1)
            .mount(&uc)
            .await;

        let response = build_app(uc.uri())
            .oneshot(rpc_request(case.service, case.rpc, case.request))
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{}/{}: {}",
            case.service,
            case.rpc,
            body_json(response).await
        );
        uc.verify().await;
    }
}
